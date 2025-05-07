import os
from ydata_profiling import ProfileReport
from ydata_profiling.config import Settings
import argparse
import pandas as pd
import hashlib
import json
import getpass
import oracledb
from dotenv import load_dotenv
import logging
import sys
from datetime import datetime

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

def connect_to_oracle(db_user: str, db_password: str, db_host: str, db_port, db_sid) -> None:
    try: 
        with oracledb.connect(user=db_user, password=db_password, dsn=oracledb.makedsn(db_host, db_port, db_sid)) as connection:
            with connection.cursor() as cursor:
                sql = """select sysdate from dual"""
                for r in cursor.execute(sql):
                    logging.info(f"Database connection successful. Current date: {r}")
    except oracledb.DatabaseError as e:
        error, = e.args
        logging.error(f"Oracle error code: {error.code}")
        logging.error(f"Oracle error message: {error.message}")
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
        with open(path_to_file, 'r') as file:
            tables = [line.strip() for line in file.readlines()]
        logging.info(f"Successfully loaded {len(tables)} tables from {path_to_file}")
        return tables
    except FileNotFoundError:
        logging.error(f"File not found at the path '{path_to_file}'. Please check the path and try again.")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading tables file: {e}")
        return []

def hash_column(column: pd.Series) -> pd.Series:
    """Hash the values in a column using SHA-256."""
    return column.apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notnull(x) else x)

def generate_profiling_report(db_connection=None, schema=None, tables_list=None, path_to_csv=None, 
                             sensitive_columns=None, sensitive_keywords=None) -> pd.DataFrame:
    """
    Generate a profiling report using ydata-profiling for either CSV files or Oracle database tables
    
    Args:
        db_connection (oracledb.Connection, optional): Connection to Oracle database
        schema (str, optional): Schema name for Oracle tables
        tables_list (list, optional): List of tables to profile
        path_to_csv (str, optional): Path to CSV file (if not using database)
        sensitive_columns (list, optional): List of column names to mark as sensitive
        sensitive_keywords (list, optional): Keywords to detect sensitive columns
        
    Returns:
        DataFrame: DataFrame with profiling information including completeness metrics
    """
    # Default sensitive keywords if not provided
    if sensitive_keywords is None:
        sensitive_keywords = ["bvn", "id number", "nin", "passport", "driver", 
                             "identificationnumber", "chn"]
    
    results_dfs = []  # To store results from multiple tables
    
    try:
        # Determine source type (CSV or database)
        if path_to_csv:
            # Process CSV file
            file_name = str.split(path_to_csv, "/")[-1].split(".")[0]
            logging.info(f"Reading CSV file: {path_to_csv}")
            data = pd.read_csv(path_to_csv)
            logging.info(f"CSV file loaded with {len(data)} records and {len(data.columns)} columns")
            source_name = file_name
            table_name = file_name
            schema_name = None
            
            # Process single CSV
            result_df = _process_dataset(data, source_name, table_name, schema_name, 
                               sensitive_columns, sensitive_keywords)
            results_dfs.append(result_df)
            
        elif db_connection and tables_list:
            # Process Oracle tables
            for table in tables_list:
                try:
                    logging.info(f"Processing table: {schema}.{table}")
                    query = f"SELECT * FROM {schema}.{table}"
                    data = pd.read_sql(query, db_connection)
                    
                    # For empty tables
                    if data.empty:
                        logging.warning(f"Table {schema}.{table} is empty. Skipping.")
                        continue
                    
                    result_df = _process_dataset(data, f"{schema}.{table}", table, schema,
                               sensitive_columns, sensitive_keywords, connection=db_connection)
                    results_dfs.append(result_df)
                    
                except Exception as e:
                    logging.error(f"Error processing table {schema}.{table}: {e}")
        else:
            logging.error("Error: Either provide path_to_csv or both db_connection and tables_list")
            return pd.DataFrame()
        
        # Combine all results
        if results_dfs:
            combined_df = pd.concat(results_dfs)
            return combined_df
        else:
            return pd.DataFrame()
        
    except Exception as e:
        logging.error(f"An unexpected error occurred in generate_profiling_report: {e}")
        return pd.DataFrame()

def _process_dataset(data, source_name, table_name, schema_name, sensitive_columns, sensitive_keywords, connection=None):
    """Helper function to process a single dataset (CSV or DB table)"""
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
                logging.info(f"Hashing sensitive column: {col}")
                data[col] = hash_column(data[col])
    
    # Configure settings to mark sensitive columns
    config = Settings()
    if sensitive_columns:
        config.variables.descriptions = {col: "Sensitive Data (Hashed)" 
                                       for col in sensitive_columns 
                                       if col in data.columns}
    
    # Generate profiling report    
    profile = ProfileReport(
        data,
        title=f"{source_name} Profiling Report",
        explorative=True,
        config=config)
    
    # Get JSON data and extract variables data
    json_data = profile.to_json()
    variables_data = json.loads(json_data)['variables']
    variables_df = pd.DataFrame(variables_data).transpose()
    variables_df = variables_df.reset_index().rename(columns={'index': 'column_name'})
    
    # Add metadata enrichment
    variables_df['table_name'] = table_name
    variables_df['schema_name'] = schema_name
    variables_df['total_records'] = total_records
    variables_df['created_at'] = pd.Timestamp.now()
    variables_df['last_updated'] = pd.Timestamp.now()
    
    # Calculate completeness percentage
    if 'count' in variables_df.columns and 'n_missing' in variables_df.columns:
        variables_df['completeness_pct'] = ((variables_df['count'] - variables_df['n_missing']) / 
                                         variables_df['count'] * 100).round(2)
    
    # Mark sensitive columns in the metadata
    variables_df['is_sensitive'] = variables_df['column_name'].isin(sensitive_columns)
    
    logging.info(f"Successfully generated profile for {source_name} with {len(variables_df)} columns")
    
    # Enrich with database metadata if connection available
    if connection and schema_name and table_name:
        try:
            logging.info(f"Retrieving database metadata for {schema_name}.{table_name}")
            db_metadata = get_column_metadata(connection, schema_name, table_name)
            
            if not db_metadata.empty:
                # Convert column names to lowercase for joining
                variables_df['column_name_lower'] = variables_df['column_name'].str.lower()
                db_metadata['db_column_name_lower'] = db_metadata['db_column_name'].str.lower()
                
                # Merge the dataframes on column name (case-insensitive)
                variables_df = pd.merge(
                    variables_df, 
                    db_metadata,
                    how='left',
                    left_on='column_name_lower',
                    right_on='db_column_name_lower'
                )
                
                # Remove the temporary columns used for joining
                variables_df = variables_df.drop(['column_name_lower', 'db_column_name_lower', 'db_column_name'], axis=1, errors='ignore')
                
                logging.info(f"Added database metadata for {len(db_metadata)} columns")
            else:
                logging.warning("No database dictionary metadata found")
        except Exception as e:
            logging.warning(f"Failed to enrich with database metadata: {e}")
            
    return variables_df

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

def generate_metadata_file(var_df: pd.DataFrame, output_path: str = None) -> pd.DataFrame:
    """
    Generate a metadata file from the profiling report.
    If the file exists, checks for duplicates and appends only new records.
    
    Args:
        var_df (DataFrame): DataFrame with profiling information
        output_path (str, optional): Path to save the metadata file
    
    Returns:
        DataFrame: The processed metadata DataFrame that was saved
    """
    try:
        # Create a copy to avoid modifying the original DataFrame
        metadata_df = var_df.copy()
        
        # Set default filename based on schema and table if available
        if output_path is None:
            if 'schema_name' in metadata_df.columns and 'table_name' in metadata_df.columns:
                # Use first row's schema and table (assuming all rows are for same table)
                schema = metadata_df['schema_name'].iloc[0]
                table = metadata_df['table_name'].iloc[0]
                if schema and table:
                    output_path = f"{schema}_{table}_metadata.csv"
                else:
                    output_path = "metadata_report.csv"
            else:
                output_path = "metadata_report.csv"
        
        logging.info(f"Preparing metadata file: {output_path}")
        
        # Rename columns if they have their original names
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
        
        # Ensure these columns are included at the beginning
        priority_cols = ['schema_name', 'table_name', 'column_name', 'data_type', 
                        'total_records', 'total_count', 'missing_count', 
                        'completeness_pct', 'is_sensitive',
                        # Add database dictionary columns
                        'position', 'data_length', 'data_precision', 'data_scale',
                        'nullable', 'required', 'comments',
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
                
        # Reorder columns
        metadata_df = metadata_df[all_cols]
        
        # Check if file exists and handle append logic
        if os.path.exists(output_path):
            logging.info(f"Metadata file {output_path} already exists. Checking for new records...")
            # Read existing metadata
            existing_df = pd.read_csv(output_path)
            
            # Define key columns for identifying duplicates
            # A row is considered a duplicate if schema, table, and column name match
            key_columns = ['schema_name', 'table_name', 'column_name']
            key_columns = [col for col in key_columns if col in metadata_df.columns and col in existing_df.columns]
            
            if key_columns:  # Only proceed with duplicate check if we have key columns
                # Filter out rows that already exist in the file
                # Create a set of tuples with the key values from existing data
                existing_keys = set(
                    tuple(row) for row in existing_df[key_columns].itertuples(index=False, name=None)
                )
                
                # Filter new data to only include rows with new keys
                new_data_mask = ~metadata_df.apply(
                    lambda row: tuple(row[key_columns]) in existing_keys, axis=1
                )
                
                # If we have any new data, append it
                if new_data_mask.any():
                    metadata_df = metadata_df[new_data_mask]
                    # Append new data to existing file
                    metadata_df.to_csv(output_path, mode='a', header=False, index=False)
                    logging.info(f"Appended {new_data_mask.sum()} new records to {output_path}")
                else:
                    logging.info(f"No new records to append to {output_path}")
                    
                # Combine for return value
                metadata_df = pd.concat([existing_df, metadata_df])
            else:
                # If we can't determine duplicates, append all (might cause duplicates)
                metadata_df.to_csv(output_path, mode='a', header=False, index=False)
                logging.warning(f"Appended all records to {output_path} (duplicate checking unavailable)")
                
        else:
            # File doesn't exist, create new
            metadata_df.to_csv(output_path, index=False)
            logging.info(f"Created new metadata file at {output_path}")
        
        return metadata_df
        
    except Exception as e:
        logging.error(f"An error occurred while generating the metadata file: {e}")
        return var_df  # Return original DataFrame if we encounter an error

def main():
    # Set up argument parser with description
    parser = argparse.ArgumentParser(
        description="Generate data profiling reports and metadata for CSV files or Oracle database tables."
    )
    
    # Create mutually exclusive group for data source (either CSV or DB)
    source_group = parser.add_mutually_exclusive_group(required=True)
    
    # CSV file option
    source_group.add_argument(
        '--csv', 
        type=str, 
        help="Path to CSV file to profile"
    )
    
    # Database connection options
    source_group.add_argument(
        '--db',
        action='store_true',
        help="Use Oracle database connection instead of CSV"
    )
    
    # Database specific arguments
    db_group = parser.add_argument_group('Database options', 'Options for Oracle database connection')
    db_group.add_argument(
        '--schema',
        type=str,
        help="Oracle schema name (required with --db)"
    )
    db_group.add_argument(
        '--tables_file',
        type=str,
        help="Path to file containing table names to profile (one per line)"
    )
    db_group.add_argument(
        '--env_file',
        type=str,
        default='.env',
        help="Path to .env file with database credentials"
    )
    db_group.add_argument(  # Add thick mode option
        '--thick',
        action='store_true',
        help="Use Oracle thick mode client instead of thin mode"
    )
    
    # Additional options
    parser.add_argument(
        '--output',
        type=str,
        help="Path to save metadata file (default based on input source)"
    )
    parser.add_argument(
        '--no_hash',
        action='store_true',
        help="Skip sensitive data hashing"
    )
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
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set up logging
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level=log_level, log_file=args.log_file)
    
    # Process based on source type
    if args.csv:
        # CSV path provided
        logging.info(f"Profiling CSV file: {args.csv}")
        df = generate_profiling_report(path_to_csv=args.csv)
        
        if df.empty:
            logging.error("Failed to generate profiling report.")
            return 1
        
        # Generate metadata file
        if args.output:
            generate_metadata_file(df, args.output)
        else:
            generate_metadata_file(df)
        
    elif args.db:
        # Database connection requested
        if not args.schema:
            logging.error("--schema is required when using --db")
            return 1
        
        if not args.tables_file:
            logging.error("--tables_file is required when using --db")
            return 1
            
        # Load environment variables for DB connection
        load_dotenv(args.env_file)
        
        # Get database credentials from environment variables
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_sid = os.getenv('DB_SID')
        
        # Check for missing credentials
        if not all([db_user, db_password, db_host, db_port, db_sid]):
            logging.error("Missing database credentials in environment variables.")
            logging.error("Required: DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_SID")
            return 1
        
        # Connect to Oracle
        try:
            # Initialize Oracle thick client if requested
            if args.thick:
                logging.info("Using Oracle thick mode client...")
                try:
                    oracledb.init_oracle_client()
                    logging.info("Oracle thick client initialized successfully")
                except Exception as e:
                    logging.warning(f"Failed to initialize Oracle thick client: {e}")
                    logging.info("Falling back to thin mode...")
            
            # Connect to database
            connection = oracledb.connect(
                user=db_user,
                password=db_password,
                dsn=oracledb.makedsn(db_host, db_port, db_sid)
            )
            logging.info(f"Successfully connected to Oracle database ({db_host}:{db_port}/{db_sid})")
            
            # Get list of tables
            tables = get_list_of_tables(args.tables_file)
            if not tables:
                logging.error("No tables found in the specified file.")
                return 1
                
            logging.info(f"Found {len(tables)} tables to profile.")
            
            # Generate profiling report for database tables
            df = generate_profiling_report(
                db_connection=connection,
                schema=args.schema,
                tables_list=tables
            )
            
            if df.empty:
                logging.error("Failed to generate profiling report.")
                return 1
            
            # Generate metadata file
            if args.output:
                generate_metadata_file(df, args.output)
            else:
                generate_metadata_file(df)
                
            # Close connection
            connection.close()
            
        except oracledb.DatabaseError as e:
            error, = e.args
            logging.error(f"Oracle error code: {error.code}")
            logging.error(f"Oracle error message: {error.message}")
            return 1
        except Exception as e:
            logging.error(f"Error establishing database connection: {e}")
            return 1
    
    logging.info("Process completed successfully!")
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
