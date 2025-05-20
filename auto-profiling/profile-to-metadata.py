import os
from ydata_profiling import ProfileReport
from ydata_profiling.config import Settings
import argparse
import pandas as pd
import hashlib
import json
import getpass
from dotenv import load_dotenv
from clickhouse_driver import Client 
import logging
import sys
from datetime import datetime

# --- Configuration Paths ---
# Calculate the path to the script's directory and the project root
script_dir = os.path.dirname(__file__)
project_root = os.path.join(script_dir, '..')

# Calculate the path to the table list file relative to the project root
tables_path = os.path.join(project_root, 'scripts', 'flex11_table_list.txt') # Renamed to match usage in main block

# Calculate the path to the output directory relative to the project root
metadata_dir = os.path.join(project_root, 'metadata_profile')

# Load environment variables from the specified secrets file
secrets_path = os.path.join(project_root, 'env', 'clickh_secrets.env')

# Calculate path to the directory containing sensitive column names
sensitive_col_path = os.path.join(project_root, 'scripts', 'sensitive_columns.txt') # currently does not exist, expecting function to return None

# Load environment variables from the .env file
# Use override=True to ensure variables in this file take precedence if they exist elsewhere
load_dotenv(dotenv_path=secrets_path, override=True)

def setup_logging(log_level=logging.INFO, log_file=None):
    """
    Configure logging for the script.
    
    Args:
        log_level (int): Logging level (default: logging.INFO)
        log_file (str, optional): Path to log file. If None, creates a default log file
                                 in the current directory.
    """
    # Create a default log file if none specified
    if log_file is None:
        script_name = os.path.basename(__file__).split('.')[0]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"{script_name}_{timestamp}.log"
    
    # Create logs directory if logging to file and directory doesn't exist
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configure basic logging
    handlers = [logging.FileHandler(log_file), logging.StreamHandler()]
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    # Log initialization
    logging.info("Logging initialized")
    logging.info(f"Log file created at: {os.path.abspath(log_file)}")
    logging.debug(f"Python version: {sys.version}")
    logging.debug(f"Script started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def connect_to_clickhouse(host: str, port: int, user: str, password: str): # Removed database parameter from function signature
    """
    Connect to ClickHouse database (using default database).

    Args:
        host (str): ClickHouse host address
        port (int): ClickHouse port
        user (str): ClickHouse username
        password (str): ClickHouse password
        # Removed database parameter from docstring as it's not compulsory for connection

    Returns:
        clickhouse_driver.Client: ClickHouse client connection object, or None if connection fails
    """
    try:
        # Use the Client object for connection
        client = Client(
            host=host,
            port=int(port), # Explicitly convert port to int here for the client
            user=user,
            password=password
            # Removed database parameter from Client constructor
        )
        # Test the connection by executing a simple query
        client.execute('SELECT 1')
        logging.info(f"Successfully connected to ClickHouse at {host}:{port}")
        return client # Return the client object on success
    except Exception as e:
        logging.error(f"Error connecting to ClickHouse: {e}") 
        return None

def get_list_of_tables(path_to_file: str) -> list:
    """
    Get a list of tables from a file.
    
    Args:
        path_to_file (str): path to file containing table names
        
    Returns:
        list: List of table names
    """
    try:
        # Ensure the file path is valid
        if not os.path.exists(path_to_file):
             logging.error(f"Error: Table list file not found at the path '{path_to_file}'. Please check the path and try again.") # CHANGED: Replaced print with logging.error
             return []
        
        with open(path_to_file, 'r') as file:
            tables = [line.strip() for line in file.readlines() if line.strip()]
        logging.info(f"Successfully loaded {len(tables)} tables from {path_to_file}")
        return tables
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading tables file: {e}")
        return []

def hash_column(column: pd.Series) -> pd.Series:
    """Hash the values in a column using SHA-256."""
    return column.apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notnull(x) else x)

# Function to read sensitive columns from a file
def get_list_sensitive_col(file_path: str) -> list | None:
    """
    Reads sensitive column names from a text file, one column name per line.

    Args:
        file_path (str): The path to the file containing sensitive column names.

    Returns:
        list: A list of sensitive column names if the file is found and readable.
        None: If the file does not exist or an error occurs during reading.
    """
    try:
        if not os.path.exists(file_path):
            logging.info(f"Sensitive columns file not found at: {file_path}. Will use keyword detection as fallback.")
            return None
        
        with open(file_path, 'r') as f:
            columns = [line.strip() for line in f if line.strip()]
        
        if columns:
            logging.info(f"Successfully loaded {len(columns)} sensitive columns from {file_path}.")
            return columns
        else:
            logging.info(f"Sensitive columns file at {file_path} is empty. Will use keyword detection as fallback.")
            return None
    except Exception as e:
        logging.error(f"Error reading sensitive columns file {file_path}: {e}. Will use keyword detection as fallback.")
        return None
    
def generate_profiling_report(db_connection: Client, tables_list: list, 
                              sensitive_columns=None, sensitive_keywords=None) -> pd.DataFrame:
    """
    Generate a profiling report using ydata-profiling for Clickhouse database tables
    
    Args:
        db_connection (clickhouse_driver.Client): Connected ClickHouse client object. 
        tables_list (list): List of table names.
        sensitive_columns (list, optional): List of column names to mark as sensitive. Expected to return None for now and sensitive keywords will be used for detection
        sensitive_keywords (list, optional): Keywords to detect sensitive columns.
        
    Returns:
        DataFrame: DataFrame with profiling information
    """
    # Default sensitive keywords if not provided
    if sensitive_keywords is None:
        sensitive_keywords = ["bvn", "id number", "nin", "passport", "driver", 
                             "identificationnumber", "chn"]
    
    results_dfs = []  # To store results from multiple tables
    
    # Check to ensure db_connection is a Client object
    if not isinstance(db_connection, Client):
        logging.error("Error: Invalid database connection object provided to generate_profiling_report. Expected clickhouse_driver.Client.") # CHANGED: Replaced print with logging.error
        return pd.DataFrame()
    
    try:
        if db_connection and tables_list:
            # Process Clickhouse tables
           for table_name in tables_list: # Iterate directly over table names
                try:
                    full_table_name = table_name # Use table_name directly
                    db_name = db_connection.database # Get the database name from the client object (will be default if not specified)

                    logging.info(f"Processing table: {full_table_name}")

                    # Construct the SQL query for ClickHouse
                    # This query uses the table_name, relying on the default database connection
                    query = f"SELECT * FROM {table_name}" # Use table_name directly in query

                    # Execute query using clickhouse_driver.Client.execute()
                    # execute returns a list of tuples, need to get column names separately
                    data_tuples = db_connection.execute(query) # Using client.execute()

                    # Get column names from the table description using client.execute()
                    column_names = [col[0] for col in db_connection.execute(f"DESCRIBE TABLE {table_name}")] # CHANGED: Use table_name in DESCRIBE TABLE query

                    # Create pandas DataFrame
                    data = pd.DataFrame(data_tuples, columns=column_names)
                    
                    # For empty tables
                    if data.empty:
                        logging.warning(f"Table {full_table_name} is empty. Skipping.")
                        continue
                    
                    result_df = _process_dataset(data, full_table_name, table_name, db_name, # CHANGED: Pass db_name
                                               sensitive_columns, sensitive_keywords)
                    results_dfs.append(result_df)

                except Exception as e:
                    logging.error(f"Error processing table {full_table_name}: {e}")
        else: # This covers cases where db_connection or tables_list are missing
            logging.error("Error: db_connection and tables_list must be provided for database profiling.") # CHANGED: Updated error message and used logging.error
            return pd.DataFrame()

        # Combine all results
        if results_dfs:
            # Use ignore_index=True to reset index when concatenating
            combined_df = pd.concat(results_dfs, ignore_index=True)
            return combined_df
        else:
            return pd.DataFrame()

    except Exception as e:
        logging.error(f"An unexpected error occurred in generate_profiling_report: {e}") # CHANGED: Replaced print with logging.error
        return pd.DataFrame()

def _process_dataset(data, source_name, table_name, schema_name, sensitive_columns, sensitive_keywords):
    """Helper function to process a single dataset (DB table)"""
    # Store the total record count
    total_records = len(data)

    # Automatically detect sensitive columns if not provided
    if sensitive_columns is None:
        sensitive_columns = [
            col for col in data.columns
            if any(keyword in col.lower() for keyword in sensitive_keywords)
        ]

    # Hash sensitive columns
    if sensitive_columns:
        for col in sensitive_columns:
            if col in data.columns:
                logging.info(f"Hashing sensitive column: {col}") # CHANGED: Replaced print with logging.info
                data[col] = hash_column(data[col])

    # Configure settings to mark sensitive columns
    config = Settings()
    if sensitive_columns:
        config.variables.descriptions = {col: "Sensitive Data (Hashed)"
                                       for col in sensitive_columns
                                       if col in data.columns}

    # Generate profiling report
    # Suppress the default HTML report generation if only JSON is needed
    profile = ProfileReport(
        data,
        title=f"{source_name} Profiling Report",
        explorative=True,
        config=config,
        # Set to False if you only need the JSON output for metadata
        # to_file=None
        )

    # Get JSON data and extract variables data
    # Use to_json() directly if to_file is None
    json_data_str = profile.to_json()
    json_data = json.loads(json_data_str)

    # Check if 'variables' key exists before accessing it
    if 'variables' not in json_data:
        logging.warning(f"Warning: 'variables' key not found in profiling report JSON for {source_name}. Skipping metadata extraction.") # CHANGED: Replaced print with logging.warning
        return pd.DataFrame() # Return empty DataFrame if no variable data

    variables_data = json_data['variables']
    variables_df = pd.DataFrame(variables_data).transpose()
    variables_df = variables_df.reset_index().rename(columns={'index': 'column_name'})

    # Add metadata enrichment
    variables_df['table_name'] = table_name
    variables_df['schema_name'] = schema_name # This will be the database name for ClickHouse
    variables_df['total_records'] = total_records
    variables_df['created_at'] = pd.Timestamp.now()
    variables_df['last_updated'] = pd.Timestamp.now()

    # Calculate completeness percentage
    # Ensure columns exist before calculating
    if 'count' in variables_df.columns and 'n_missing' in variables_df.columns:
        # Avoid division by zero if count is 0
        variables_df['completeness_pct'] = variables_df.apply(
            lambda row: ((row['count'] - row['n_missing']) / row['count'] * 100).round(2) if row['count'] > 0 else 0,
            axis=1
        )
    else:
         variables_df['completeness_pct'] = 0 # Default to 0 if columns are missing


    # Mark sensitive columns in the metadata
    variables_df['is_sensitive'] = variables_df['column_name'].isin(sensitive_columns)

    logging.info(f"Successfully generated profile for {source_name} with {len(variables_df)} columns")

    return variables_df
'''
def get_column_metadata(connection, schema, table):
    """
    Get additional metadata from Oracle data dictionary
    
    Args:
        connection: Oracle database connection
        schema: Schema name
        table: Table name
        
    Returns:
        DataFrame: Database dictionary metadata for the table
    """
    try:
        query = """
        SELECT 
            column_name, 
            data_type, 
            data_length, 
            data_precision,
            data_scale,
            nullable, 
            column_id AS position,
            CASE WHEN nullable = 'N' THEN 'YES' ELSE 'NO' END AS required,
            comments
        FROM 
            all_tab_columns a
        LEFT JOIN 
            all_col_comments b
        ON 
            a.owner = b.owner 
            AND a.table_name = b.table_name 
            AND a.column_name = b.column_name
        WHERE 
            a.owner = :schema 
            AND a.table_name = :table
        ORDER BY 
            column_id
        """
        db_metadata = pd.read_sql(
            query, 
            connection, 
            params={'schema': schema.upper(), 'table': table.upper()}
        )
        
        # Convert column names to lowercase for consistency
        db_metadata.columns = [col.lower() for col in db_metadata.columns]
        
        # Rename column_name to match our metadata structure
        db_metadata = db_metadata.rename(columns={'column_name': 'db_column_name'})
        
        return db_metadata
    except Exception as e:
        logging.error(f"Error retrieving database metadata: {e}")
        return pd.DataFrame()
'''

def generate_metadata_file(var_df: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    """
    Generate a metadata file from the profiling report and save it to a specified directory.
    If the file exists, checks for duplicates and appends only new records.

    Args:
        var_df (DataFrame): DataFrame with profiling information
        output_dir (str): Path to the directory to save the metadata file # CHANGED: Parameter name changed to output_dir

    Returns:
        DataFrame: The processed metadata DataFrame that was saved
    """
    try:
        # Create the output directory if it doesn't exist
        # os.makedirs(output_dir, exist_ok=True)

        # Create a copy to avoid modifying the original DataFrame
        metadata_df = var_df.copy()

        # Determine the filename based on table name
        # Ensure metadata_df is not empty and has the required columns
        if metadata_df.empty or 'table_name' not in metadata_df.columns:
             logging.error("Error: Metadata DataFrame is empty or missing required column ('table_name'). Cannot determine filename.") 
             return pd.DataFrame() # Return empty DataFrame if essential info is missing

        # Use first row's schema and table (assuming all rows are for same table for a single file)
        # CHANGED: Removed schema retrieval
        table = metadata_df['table_name'].iloc[0]
        # CHANGED: Check only for table
        if not table:
             logging.error("Error: Table name is missing in the metadata. Cannot determine filename.") # CHANGED: Replaced print with logging.error
             return pd.DataFrame()

        # Construct the full output file path within the output directory
        # Construct filename using only table name
        output_filename = f"{table}_metadata.csv"
        output_path = os.path.join(output_dir, output_filename) # Construct full path using output_dir

        logging.info(f"Preparing metadata file: {output_path}")

        # Rename columns if they have their original names (unchanged from previous versions)
        rename_map = {
            'n_distinct': 'distinct_count',
            'p_distinct': 'distinct_percentage',
            'is_unique': 'is_unique',
            'type': 'data_type',
            'n_unique': 'unique_count',
            'p_unique': 'unique_percentage',
            'n_missing': 'missing_count',
            'n': 'total_count',
            'p_missing': 'missing_percentage',
            'n_category': 'category_count'
        }

        # Only rename columns that exist and haven't been renamed yet
        rename_cols = {k: v for k, v in rename_map.items() if k in metadata_df.columns and v not in metadata_df.columns}
        if rename_cols:
            metadata_df.rename(columns=rename_cols, inplace=True)

        # Ensure these columns are included at the beginning (unchanged from previous versions)
        priority_cols = ['schema_name', 'table_name', 'column_name', 'data_type',
                        'total_records', 'total_count', 'missing_count',
                        'completeness_pct', 'is_sensitive',
                        'created_at', 'last_updated']

        # Create a list of all columns with priority columns first
        all_cols = []
        for col in priority_cols:
            if col in metadata_df.columns:
                all_cols.append(col)

        # Add remaining columns
        for col in metadata_df.columns:
            if col not in all_cols:
                all_cols.append(col)

        # Reorder columns, handling cases where some priority columns might be missing
        # Ensure all_cols only contains columns actually present in metadata_df
        all_cols = [col for col in all_cols if col in metadata_df.columns]
        metadata_df = metadata_df[all_cols]


        # Check if file exists and handle append logic (unchanged from previous versions)
        if os.path.exists(output_path):
            logging.info(f"Metadata file {output_path} already exists. Checking for new records...")
            # Read existing metadata
            existing_df = pd.read_csv(output_path)

            # Define key columns for identifying duplicates
            key_columns = ['schema_name', 'table_name', 'column_name']
            # Filter key_columns to only include those present in *both* dataframes
            key_columns = [col for col in key_columns if col in metadata_df.columns and col in existing_df.columns]


            if key_columns:
                existing_keys = set(
                    tuple(row) for row in existing_df[key_columns].itertuples(index=False, name=None)
                )

                new_data_mask = ~metadata_df.apply(
                    lambda row: tuple(row[[col for col in key_columns if col in metadata_df.columns]]) in existing_keys, axis=1
                )

                if new_data_mask.any():
                    new_records_df = metadata_df[new_data_mask]
                    new_records_df.to_csv(output_path, mode='a', header=False, index=False)
                    logging.info(f"Appended {new_data_mask.sum()} new records to {output_path}") 
                    metadata_df = pd.concat([existing_df, new_records_df], ignore_index=True)
                else:
                    logging.info(f"No new records to append to {output_path}")
                    metadata_df = existing_df

            else:
                metadata_df.to_csv(output_path, mode='a', header=False, index=False)
                logging.warning(f"Appended all records to {output_path} (duplicate checking unavailable)")
                metadata_df = pd.read_csv(output_path)

        else:
            # File doesn't exist, create new
            metadata_df.to_csv(output_path, index=False)
            logging.info(f"Created new metadata file at {output_path}") # CHANGED: Replaced print with logging.info

        return metadata_df

    except Exception as e:
        logging.error(f"An error occurred while generating the metadata file: {e}") # CHANGED: Replaced print with logging.error
        return var_df  # Return original DataFrame if we encounter an error

# Example Usage:
if __name__ == "__main__":
    # CHANGED: Added argument parsing for log file and log level
    # import argparse # Already imported above
    parser = argparse.ArgumentParser(
        description="Generate data profiling reports and metadata for ClickHouse database tables."
    )

    # Added ClickHouse specific arguments if needed (currently using env vars)
    # parser.add_argument('--ch_host', type=str, help='ClickHouse host')
    # parser.add_argument('--ch_port', type=int, help='ClickHouse port')
    # parser.add_argument('--ch_user', type=str, help='ClickHouse user')
    # parser.add_argument('--ch_password', type=str, help='ClickHouse password')
    # parser.add_argument('--ch_database', type=str, help='ClickHouse database') # If not using default

    parser.add_argument(
        '--tables_file',
        type=str,
        default=tables_path, # Set default to the calculated path
        help="Path to file containing table names to profile (one per line)"
    )

    parser.add_argument(
        '--output_dir', # Changed argument name to output_dir
        type=str,
        default=metadata_dir, # Set default to the calculated path
        help="Path to the directory to save metadata files"
    )

    parser.add_argument(
        '--env_file',
        type=str,
        default=secrets_path, # Set default to the calculated secrets path
        help="Path to .env file with database credentials"
    )

    # Added logging arguments
    parser.add_argument(
        '--log_file',
        type=str,
        help="Path to log file (if not specified, a timestamped log file is created automatically)"
    )
    parser.add_argument(
        '--log_level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Logging level (default: INFO)"
    )

    # Removed sensitive column arguments from argparse if handled by env vars

    args = parser.parse_args()

    # Set up logging based on parsed arguments
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level=log_level, log_file=args.log_file) # CHANGED: Call setup_logging

    # --- ClickHouse Connection Details ---
    # Read credentials from environment variables loaded from the secrets file
    # Assuming secrets file uses variable names: host, port, user, password, database
    ch_host = os.getenv('host')
    # Read port as string first using the same variable name
    ch_port = os.getenv('port')
    ch_user = os.getenv('user')
    ch_password = os.getenv('password')
    # Removed ch_database retrieval as it's not used for connection
'''
    # Convert port to integer, handle potential errors
    # Port must be provided and be a valid integer
    ch_port = None # Initialize as None
    if ch_port is None:
        logging.error("Error: ClickHouse port ('port') not found in environment variables loaded from the secrets file.")
        # exit(1) # Exit if port is not set --prodcued an error, hence commented out as else statement did not trigger
    else:
        try:
            ch_port = int(ch_port) # Convert the string to an integer
        except ValueError:
            logging.error(f"Error: Invalid port value '{ch_port}' in secrets file. Port must be an integer.")
            exit(1) # Exit if port is invalid
'''

    # 1. Validate essential configuration
    # Check for connection details and required file paths
    # Check if ch_port is not None (it will be None only if os.getenv('port') returned None)
    if not all([ch_host, ch_port is not None, ch_user, ch_password]):
        logging.error("Error: Essential ClickHouse connection details (host, port, user, password) not found or invalid in environment variables loaded from the secrets file.") # CHANGED: Replaced print with logging.error
        # Print the original string value of port if available, for better error message
        logging.error(f"Host: {ch_host}, Port: {os.getenv('port')}, User: {ch_user}")
        exit(1) # Exit if essential connection details is missing

    # Check if the calculated paths for inputs/outputs seem valid
    # Use args.tables_file which is set by argparse (defaulting to tables_path)
    if not os.path.exists(args.tables_file):
        logging.error(f"Error: Table list file path does not exist: {args.tables_file}")
        exit(1)

    # We don't need to check if metadata_dir exists here, generate_metadata_file creates it.
    # But ensure the variable isn't empty
    # CHANGED: Use args.output_dir which is set by argparse (defaulting to metadata_dir)
    if not args.output_dir:
         logging.error("Error: Calculated metadata output directory path is empty.")
         exit(1)
    # CHANGED: Added check to create the output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    logging.info(f"Ensured output directory exists: {args.output_dir}")

    # --- Sensitive Column Handling ---
    # Attempt to load sensitive columns from a file first
    sensitive_cols_from_file = get_list_sensitive_col(sensitive_col_path)

    # Define keywords for automatic detection (fallback)
    sensitive_keyword_list = ["bvn", "id number", "nin", "passport", "driver",
                                 "identificationnumber", "chn"]


    # 2. Connect to ClickHouse
    # Pass all connection details, excluding the database name
    # CHANGED: Removed ch_database from the connection function call
    ch_client = connect_to_clickhouse(
        host=ch_host,
        port=ch_port, # Pass the integer port
        user=ch_user,
        password=ch_password
    )


    if ch_client: # This condition now correctly checks if a Client object was returned
        # 3. Get list of tables from the file
        # Use the calculated TABLES_FILE_PATH
        # CHANGED: Use args.tables_file
        tables_to_profile = get_list_of_tables(args.tables_file)

        if tables_to_profile:
            # 4. Generate profiling report for ClickHouse tables
            # Pass db_connection (the Client object) and tables_list
            profiling_results_df = generate_profiling_report(
                db_connection=ch_client, # Pass the Client object
                tables_list=tables_to_profile,
                sensitive_columns=sensitive_cols_from_file, # Expected to be a list from a file. However, this will be None for now
                sensitive_keywords=sensitive_keyword_list # Use the keyword list for detection
            )

            # 5. Generate and save the metadata file to the specified directory
            if not profiling_results_df.empty:
                # Pass the calculated METADATA_OUTPUT_DIR
                # Use args.output_dir
                generate_metadata_file(profiling_results_df, output_dir=args.output_dir)
            else:
                logging.info("No profiling results to generate metadata file.")

        else:
            logging.info("No tables found in the specified file to profile.")

        # Close the ClickHouse connection
        # clickhouse-driver Client objects have a close() method
        try:
            ch_client.close() # Use the close() method for Client
            logging.info("ClickHouse connection closed.")
        except Exception as e:
            logging.error(f"Error closing ClickHouse connection: {e}")

    else:
        logging.error("Failed to connect to ClickHouse. Cannot proceed with profiling.")

    logging.info("Script finished.")
