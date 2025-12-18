import os
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import dotenv_values

# Project root
def find_project_root(start_path):
    current_path = os.path.abspath(start_path)
    while True:
        if os.path.exists(os.path.join(current_path, ".git")):  # Search for .git to indicate root
            return current_path
        parent_path = os.path.dirname(current_path)
        if parent_path == current_path:  # Reach file system root.
            return None
        current_path = parent_path

# Load .env file values (cached per execution)
_env_values = None

def _load_env_file():
    """Load .env file values once and cache them"""
    global _env_values
    if _env_values is None:
        start_path = os.path.dirname(os.path.abspath(__file__))
        project_root = find_project_root(start_path)
        if project_root:
            env_file_path = os.path.join(project_root, ".env")
            if os.path.exists(env_file_path):
                _env_values = dotenv_values(env_file_path)
            else:
                _env_values = {}
        else:
            _env_values = {}
    return _env_values

# Get environment variable from .env file first, fallback to os.environ
def get_env_var(var_name):
    """Get environment variable from .env file first, fallback to os.environ"""
    env_values = _load_env_file()
    if var_name in env_values:
        return env_values[var_name]
    return os.environ.get(var_name)

# PostgreSQL client configuration
def setup_postgres_client():
    connection_parameters = {
        "host": get_env_var("POSTGRES_HOST"),
        "port": get_env_var("POSTGRES_PORT") or "5432",
        "database": get_env_var("POSTGRES_DATABASE"),
        "user": get_env_var("POSTGRES_USER"),
        "password": get_env_var("POSTGRES_PASSWORD")
    }

    # Check if all required parameters are set
    required_params = ["host", "database", "user", "password"]
    for key in required_params:
        if not connection_parameters[key]:
            raise ValueError(f"POSTGRES_{key.upper()} environment variable not set.")

    try:
        conn = psycopg2.connect(**connection_parameters)
        print("Successfully connected to PostgreSQL!")
        return conn
    except psycopg2.Error as e:
        raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

# YAML file config - Load from dynamic_models.yml
def load_yaml_config():
    start_path = os.path.dirname(os.path.abspath(__file__))  # Start search from current script directory.
    project_root = find_project_root(start_path)
    if not project_root:
        raise FileNotFoundError("Project root (indicated by .git directory) not found.")
    yaml_file_path = os.path.join(project_root, "models", "dynamic_models.yml")
    if not os.path.exists(yaml_file_path):
        raise FileNotFoundError(f"YAML config file not found at: {yaml_file_path}")
    with open(yaml_file_path, 'r') as yaml_file:
        return yaml.safe_load(yaml_file)

# PostgreSQL - Get customers
def get_customers_from_postgres(conn, query):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            # Fetch both CUSTOMER and CUSTOMER_ENV
            customers = [
                {"customer": row['customer'], "customer_env": row['customer_env']}
                for row in rows
            ]
            for c in customers:
                print(f"Customer: {c['customer']}, Env: {c['customer_env']}")
            return customers
    except Exception as e:
        raise RuntimeError(f"Error executing query in PostgreSQL: {e}")

# Generate SQL files from config
def generate_sql_files(conn, config):
    for model_config in config["dynamic_models"]:
        customer_query = model_config["params"][0]["query"]
        customers = get_customers_from_postgres(conn, customer_query)

        model_query = model_config["params"][1]["query"]
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(model_query)
            model_results = cursor.fetchall()
        models = [row['table_name'] for row in model_results]

        for customer_dict in customers:
            customer_name = customer_dict["customer"]
            customer_env = customer_dict["customer_env"]
            for model in models:
                output_dir = model_config["location"].format(
                    customer=customer_name,
                    customer_env=customer_env
                )
                start_path = os.path.dirname(os.path.abspath(__file__))
                project_root = find_project_root(start_path)
                if project_root:
                    output_dir_full_path = os.path.join(project_root, output_dir)
                else:
                    output_dir_full_path = output_dir

                filename = model_config["name"].format(customer=customer_name, model=model) + ".sql"
                file_path = os.path.join(output_dir_full_path, filename)

                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                sql_content = model_config["sql"].format(customer=customer_name, model=model)
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(sql_content)
                print(f"File generated: {file_path}")

# Main execution
def main():
    conn = None
    try:
        conn = setup_postgres_client()
        config = load_yaml_config()
        generate_sql_files(conn, config)
    except (ValueError, ConnectionError, FileNotFoundError, RuntimeError) as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()