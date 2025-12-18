import os
import yaml
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSessionException

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

# Snowflake client configuration
def setup_snowflake_client():
    connection_parameters = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"), 
        "user": os.environ.get("SNOWFLAKE_USERNAME"), 
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE")
    }

    # Check if all required parameters are set
    for key, value in connection_parameters.items():
        if not value:
            raise ValueError(f"SNOWFLAKE_{key.upper()} environment variable not set.")

    try:
        session = Session.builder.configs(connection_parameters).create()
        print("Successfully connected to Snowflake!")
        return session
    except SnowparkSessionException as e:
        raise ConnectionError(f"Failed to connect to Snowflake: {e}")

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

# Snowflake - Get customers
def get_customers_from_snowflake(session, query):
    try:
        results_df = session.sql(query).collect()
        # Fetch both CUSTOMER and CUSTOMER_ENV
        customers = [
            {"customer": row['CUSTOMER'], "customer_env": row['CUSTOMER_ENV']}
            for row in results_df
        ]
        for c in customers:
            print(f"Customer: {c['customer']}, Env: {c['customer_env']}")
        return customers
    except Exception as e:
        raise RuntimeError(f"Error executing query in Snowflake: {e}")

# Generate SQL files from config
def generate_sql_files(session, config):
    for model_config in config["dynamic_models"]:
        customer_query = model_config["params"][0]["query"]
        customers = get_customers_from_snowflake(session, customer_query)

        model_query = model_config["params"][1]["query"]
        model_results = session.sql(model_query).collect()
        models = [row['TABLE_NAME'] for row in model_results]

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
    session = None
    try:
        session = setup_snowflake_client()
        config = load_yaml_config()
        generate_sql_files(session, config)       
    except (ValueError, ConnectionError, FileNotFoundError, RuntimeError) as e:
        print(f"An error occurred: {e}")
    finally:
        if session:
            session.close()

if __name__ == "__main__":
    main()