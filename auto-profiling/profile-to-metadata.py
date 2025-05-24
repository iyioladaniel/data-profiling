"""
profile-to-metadata.py
----------------------
Unified data profiling script for Oracle, ClickHouse, and CSV sources.

- Generates profiling reports (HTML) and metadata (CSV) for tables or files.
- Handles sensitive columns (hashing or detection by keywords or file).
- Modular, robust, and database-agnostic.
- Usage: See CLI help (`python profile-to-metadata.py --help`).
"""
import os
import sys
import argparse
import logging
import json
import hashlib
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from ydata_profiling import ProfileReport
from ydata_profiling.config import Settings
from typing import Optional, List, Any, Union

# Optional imports for DBs
try:
    import oracledb
except ImportError:
    oracledb = None
try:
    from clickhouse_driver import Client as ClickHouseClient
except ImportError:
    ClickHouseClient = None

# --- Logging Setup ---
def setup_logging(log_level: int = logging.INFO, log_file: Optional[str] = None) -> None:
    """
    Set up logging to file and console.
    Args:
        log_level: Logging level (e.g., logging.INFO)
        log_file: Path to log file (if None, auto-generates one)
    """
    if log_file is None:
        script_name = os.path.basename(__file__).split('.')[0]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"{script_name}_{timestamp}.log"
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    handlers = [logging.FileHandler(log_file), logging.StreamHandler()]
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    logging.info("Logging initialized")
    logging.info(f"Log file created at: {os.path.abspath(log_file)}")
    logging.debug(f"Python version: {sys.version}")
    logging.debug(f"Script started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# --- Sensitive Columns ---
def get_sensitive_columns_from_file(file_path: str) -> Optional[List[str]]:
    """
    Load sensitive column names from a file (one per line).
    Returns a list of column names, or None if not found/empty.
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

def hash_column(column: pd.Series) -> pd.Series:
    """
    Hash a pandas Series using SHA-256. Used for sensitive columns.
    """
    return column.apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notnull(x) else x)

# --- DB Connections ---
def connect_to_oracle(db_user: Optional[str], db_password: Optional[str], db_host: Optional[str], db_port: Optional[Union[str, int]], db_sid: Optional[str], thick_mode: bool = False) -> Optional[Any]:
    """
    Establish a connection to an Oracle database.
    Args:
        db_user, db_password, db_host, db_port, db_sid: Connection parameters
        thick_mode: If True, try to use Oracle thick client
    Returns:
        Oracle connection object or None on failure
    """
    if oracledb is None:
        logging.error("oracledb package is not installed.")
        return None
    try:
        if db_user is None or db_password is None or db_host is None or db_port is None or db_sid is None:
            logging.error("One or more Oracle connection parameters are None.")
            return None
        port_int = int(db_port)
        if thick_mode:
            try:
                oracledb.init_oracle_client()
                logging.info("Oracle thick client initialized successfully")
            except Exception as e:
                logging.warning(f"Failed to initialize Oracle thick client: {e}")
                logging.info("Falling back to thin mode...")
        connection = oracledb.connect(
            user=db_user,
            password=db_password,
            dsn=oracledb.makedsn(db_host, port_int, db_sid)
        )
        logging.info(f"Successfully connected to Oracle database ({db_host}:{port_int}/{db_sid})")
        return connection
    except oracledb.DatabaseError as e:
        error, = e.args
        logging.error(f"Oracle error code: {error.code}")
        logging.error(f"Oracle error message: {error.message}")
        return None
    except Exception as e:
        logging.error(f"Error establishing Oracle connection: {e}")
        return None

def connect_to_clickhouse(host: Optional[str], port: Optional[Union[str, int]], user: Optional[str], password: Optional[str]) -> Optional[Any]:
    """
    Establish a connection to a ClickHouse database.
    Args:
        host, port, user, password: Connection parameters
    Returns:
        ClickHouse client object or None on failure
    """
    if ClickHouseClient is None:
        logging.error("clickhouse-driver package is not installed.")
        return None
    try:
        if host is None or port is None or user is None or password is None:
            logging.error("One or more ClickHouse connection parameters are None.")
            return None
        client = ClickHouseClient(
            host=host,
            port=int(port),
            user=user,
            password=password
        )
        # Test connection
        client.execute('SELECT 1')
        logging.info(f"Successfully connected to ClickHouse at {host}:{port}")
        return client
    except Exception as e:
        logging.error(f"Error connecting to ClickHouse: {e}")
        return None

# --- Table List ---
def get_list_of_tables(path_to_file: str) -> List[str]:
    """
    Read a list of table names from a file (one per line).
    Returns a list of table names.
    """
    try:
        if not os.path.exists(path_to_file):
            logging.error(f"File not found at the path '{path_to_file}'. Please check the path and try again.")
            return []
        with open(path_to_file, 'r') as file:
            tables = [line.strip() for line in file.readlines() if line.strip()]
        logging.info(f"Successfully loaded {len(tables)} tables from {path_to_file}")
        return tables
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading tables file: {e}")
        return []

# --- Profiling Logic ---
def _process_dataset(
    data: pd.DataFrame,
    source_name: str,
    table_name: str,
    schema_name: Optional[str],
    html_output_dir: Optional[str],
    sensitive_columns: Optional[List[str]],
    sensitive_keywords: Optional[List[str]],
    hash_sensitive: bool = True,
    connection: Optional[Any] = None,
    profile_type: str = "full"
) -> pd.DataFrame:
    """
    Profile a single dataset (DataFrame), generate HTML report, and extract metadata.
    Args:
        data: pandas DataFrame
        source_name: Name for report title
        table_name: Table or file name
        schema_name: Schema name (if any)
        html_output_dir: Directory to save HTML report
        sensitive_columns: List of sensitive columns (or None)
        sensitive_keywords: List of keywords for sensitive detection
        hash_sensitive: If True, hash sensitive columns
        connection: DB connection for metadata enrichment (optional)
        profile_type: 'minimal' or 'full'
    Returns:
        DataFrame with profiling metadata
    """
    total_records = len(data)
    # Detect sensitive columns if not provided
    if sensitive_keywords is None:
        sensitive_keywords = []
    if sensitive_columns is None:
        sensitive_columns = [
            col for col in data.columns
            if any(keyword in col.lower() for keyword in sensitive_keywords)
        ]
    # Hash sensitive columns if required
    if sensitive_columns and hash_sensitive:
        for col in sensitive_columns:
            if col in data.columns:
                logging.info(f"Hashing sensitive column: {col}")
                data[col] = hash_column(data[col])
    # Configure ydata-profiling
    config = Settings()
    if sensitive_columns:
        config.variables.descriptions = {col: "Sensitive Data (Hashed)" for col in sensitive_columns if col in data.columns}
        
    # Infer date columns and build type_schema
    type_schema = {}
    for col in data.columns:
        if 'date' in col.lower():
            type_schema[col] = "DateTime"
    
    # Generate profiling report based on profile_type
    if profile_type == "minimal":
        profile = ProfileReport(
            data,
            title=f"{source_name} Profiling Report",
            explorative=False,
            minimal=True,
            samples=None,  # Disable sample data
            correlations=None,  # Disable correlations
            missing_diagrams=None,  # Disable missing diagrams
            config=config,
            interactions=None,  # Disable interactions
            type_schema=type_schema  # Pass the type schema for date columns
        )
    else:
        profile = ProfileReport(
            data,
            title=f"{source_name} Profiling Report",
            explorative=True,
            config=config,
            type_schema=type_schema  # Pass the type schema for date columns
        )
    # Save HTML report
    if html_output_dir:
        os.makedirs(html_output_dir, exist_ok=True)
        output_filename = f"{table_name}_profiling_report.html"
        output_html_path = os.path.join(html_output_dir, output_filename)
        profile.to_file(output_html_path)
        logging.info(f"Generated profiling report for {source_name} at {output_html_path}")
    # Extract variables metadata from profiling JSON
    json_data = profile.to_json()
    variables_data = json.loads(json_data)['variables']
    variables_df = pd.DataFrame(variables_data).transpose()
    variables_df = variables_df.reset_index().rename(columns={'index': 'column_name'})
    variables_df['table_name'] = table_name
    variables_df['schema_name'] = schema_name
    variables_df['total_records'] = total_records
    variables_df['last_updated'] = pd.Timestamp.now()
    # Mark sensitive columns
    variables_df['is_sensitive'] = variables_df['column_name'].isin(sensitive_columns)
    # Optionally enrich with DB metadata
    if connection and schema_name and table_name:
        try:
            db_metadata = get_column_metadata(connection, schema_name, table_name)
            if not db_metadata.empty:
                # Merge DB metadata on column name (case-insensitive)
                variables_df['column_name_lower'] = variables_df['column_name'].str.lower()
                db_metadata['db_column_name_lower'] = db_metadata['db_column_name'].str.lower()
                variables_df = pd.merge(
                    variables_df,
                    db_metadata,
                    how='left',
                    left_on='column_name_lower',
                    right_on='db_column_name_lower'
                )
                variables_df = variables_df.drop(['column_name_lower', 'db_column_name_lower', 'db_column_name'], axis=1, errors='ignore')
                logging.info(f"Added database metadata for {len(db_metadata)} columns")
            else:
                logging.warning("No database dictionary metadata found")
        except Exception as e:
            logging.warning(f"Failed to enrich with database metadata: {e}")
    return variables_df

def get_column_metadata(connection: Any, schema: str, table: str) -> pd.DataFrame:
    """
    Retrieve column metadata from Oracle data dictionary for a given table.
    Args:
        connection: Oracle connection
        schema: Schema name
        table: Table name
    Returns:
        DataFrame with column metadata (or empty DataFrame on error)
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
        db_metadata.columns = [col.lower() for col in db_metadata.columns]
        db_metadata = db_metadata.rename(columns={'column_name': 'db_column_name'})
        return db_metadata
    except Exception as e:
        logging.error(f"Error retrieving database metadata: {e}")
        return pd.DataFrame()

def generate_profiling_report(
    db_type: str,
    db_connection: Optional[Any] = None,
    schema: Optional[str] = None,
    tables_list: Optional[List[str]] = None,
    path_to_csv: Optional[str] = None,
    html_output_dir: Optional[str] = None,
    sensitive_columns: Optional[List[str]] = None,
    sensitive_keywords: Optional[List[str]] = None,
    hash_sensitive: bool = True,
    profile_type: str = "full"
) -> pd.DataFrame:
    """
    Main entry point for profiling: handles CSV, Oracle, and ClickHouse sources.
    Args:
        db_type: 'csv', 'oracle', or 'clickhouse'
        db_connection: DB connection/client (if applicable)
        schema: Schema name (Oracle)
        tables_list: List of tables to profile
        path_to_csv: Path to CSV file (if applicable)
        html_output_dir: Directory for HTML reports
        sensitive_columns: List of sensitive columns (or None)
        sensitive_keywords: List of keywords for sensitive detection
        hash_sensitive: If True, hash sensitive columns
        profile_type: 'minimal' or 'full'
    Returns:
        DataFrame with combined profiling metadata for all sources
    """
    if sensitive_keywords is None:
        sensitive_keywords = ["bvn", "id number", "nin", "passport", "driver", "identificationnumber", "chn", "email", "phone number", "mobile number", "address"]
    results_dfs = []
    try:
        if db_type == 'csv' and path_to_csv:
            # Profile a single CSV file
            file_name = os.path.basename(path_to_csv).split(".")[0]
            logging.info(f"Reading CSV file: {path_to_csv}")
            data = pd.read_csv(path_to_csv)
            logging.info(f"CSV file loaded with {len(data)} records and {len(data.columns)} columns")
            result_df = _process_dataset(data, file_name, file_name, None, html_output_dir, sensitive_columns, sensitive_keywords, hash_sensitive, profile_type=profile_type)
            results_dfs.append(result_df)
        elif db_type == 'oracle' and db_connection and tables_list:
            # Profile each Oracle table
            for table in tables_list:
                try:
                    query = f'SELECT * FROM {schema}.{table}'
                    data = pd.read_sql(query, db_connection)
                    result_df = _process_dataset(data, f"{schema}.{table}", table, schema, html_output_dir, sensitive_columns, sensitive_keywords, hash_sensitive, connection=db_connection, profile_type=profile_type)
                    results_dfs.append(result_df)
                except Exception as e:
                    logging.error(f"Error processing table {schema}.{table}: {e}")
        elif db_type == 'clickhouse' and db_connection and tables_list:
            # Profile each ClickHouse table
            for table in tables_list:
                try:
                    table_name_list = table.split('_')
                    source = table_name_list[0]
                    if len(table_name_list) > 1:
                        table_name = '_'.join(table_name_list[1:])
                    else:
                        table_name = table_name_list[0]
                    logging.info(f"Reading ClickHouse table: {table} (source_application: {source}, table: {table_name})")
                    query = f'SELECT * FROM {table}'
                    # Get column names from DESCRIBE TABLE
                    data = pd.DataFrame(db_connection.execute(query), columns=[col[0] for col in db_connection.execute(f"DESCRIBE TABLE {table}")])
                    result_df = _process_dataset(data, source, table_name, None, html_output_dir, sensitive_columns, sensitive_keywords, hash_sensitive, profile_type=profile_type)
                    results_dfs.append(result_df)
                except Exception as e:
                    logging.error(f"Error processing ClickHouse table {table}: {e}")
        else:
            logging.error("Error: Invalid arguments for profiling. Check db_type and required parameters.")
            return pd.DataFrame()
        if results_dfs:
            combined_df = pd.concat(results_dfs, ignore_index=True)
            return combined_df
        else:
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"An unexpected error occurred in generate_profiling_report: {e}")
        return pd.DataFrame()

def generate_metadata_file(
    var_df: pd.DataFrame,
    output_dir: Optional[str] = None,
    output_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Save profiling metadata DataFrame to CSV, appending or creating as needed.
    Args:
        var_df: DataFrame with profiling metadata
        output_dir: Directory to save file (if output_path not given)
        output_path: Full path to output file (optional)
    Returns:
        DataFrame with all metadata (including appended, if any)
    """
    try:
        metadata_df = var_df.copy()
        # Determine output path
        if output_path is None:
            if output_dir is None:
                output_path = "metadata_report.csv"
            else:
                if 'schema_name' in metadata_df.columns and 'table_name' in metadata_df.columns:
                    schema = metadata_df['schema_name'].iloc[0]
                    table = metadata_df['table_name'].iloc[0]
                    if schema and table:
                        output_path = os.path.join(output_dir, f"{schema}_{table}_metadata.csv")
                    else:
                        output_path = os.path.join(output_dir, "metadata_report.csv")
                elif 'table_name' in metadata_df.columns:
                    table = metadata_df['table_name'].iloc[0]
                    output_path = os.path.join(output_dir, f"{table}_metadata.csv")
                else:
                    output_path = os.path.join(output_dir, "metadata_report.csv")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        logging.info(f"Preparing metadata file: {output_path}")
        # Rename columns for consistency
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
        rename_cols = {k: v for k, v in rename_map.items() if k in metadata_df.columns and v not in metadata_df.columns}
        if rename_cols:
            metadata_df.rename(columns=rename_cols, inplace=True)
        # Reorder columns for readability
        priority_cols = ['source', 'table_name', 'column_name', 'data_type', 'is_sensitive',
                        'total_records', 'total_count', 'missing_count', 'completeness_pct',
                        'is_unique', 'unique_count', 'unique_percentage', 'distinct_count', 'distinct_percentage',
                        'category_count', 'position', 'data_length', 'data_precision', 'data_scale',
                        'nullable', 'required', 'comments', 'created_at', 'last_updated']
        all_cols = [col for col in priority_cols if col in metadata_df.columns]
        metadata_df = metadata_df[all_cols]
        # If file exists, append only new records
        if os.path.exists(output_path):
            logging.info(f"Metadata file {output_path} already exists. Checking for new records...")
            existing_df = pd.read_csv(output_path)
            key_columns = ['schema_name', 'table_name', 'column_name']
            key_columns = [col for col in key_columns if col in metadata_df.columns and col in existing_df.columns]
            if key_columns:
                existing_keys = set(tuple(row) for row in existing_df[key_columns].itertuples(index=False, name=None))
                new_data_mask = ~metadata_df.apply(lambda row: tuple(row[key_columns]) in existing_keys, axis=1)
                if new_data_mask.any():
                    metadata_df = metadata_df[new_data_mask]
                    metadata_df.to_csv(output_path, mode='a', header=False, index=False)
                    logging.info(f"Appended {new_data_mask.sum()} new records to {output_path}")
                else:
                    logging.info(f"No new records to append to {output_path}")
                metadata_df = pd.concat([existing_df, metadata_df])
            else:
                metadata_df.to_csv(output_path, mode='a', header=False, index=False)
                logging.warning(f"Appended all records to {output_path} (duplicate checking unavailable)")
        else:
            metadata_df.to_csv(output_path, index=False)
            logging.info(f"Created new metadata file at {output_path}")
        return metadata_df
    except Exception as e:
        logging.error(f"An error occurred while generating the metadata file: {e}")
        return var_df

# --- Main CLI ---
def main() -> int:
    """
    Main command-line interface for the profiling script.
    Parses arguments, loads environment, and dispatches to profiling logic.
    """
    parser = argparse.ArgumentParser(
        description="Generate data profiling reports and metadata for CSV, Oracle, or ClickHouse tables."
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument('--csv', type=str, help="Path to CSV file to profile")
    source_group.add_argument('--db_type', type=str, choices=['oracle', 'clickhouse'], help="Database type: oracle or clickhouse")
    parser.add_argument('--schema', type=str, help="Schema name (Oracle only)")
    parser.add_argument('--tables_file', type=str, help="Path to file containing table names to profile (one per line)")
    parser.add_argument('--env_file', type=str, default=None, help="Path to .env file with database credentials")
    parser.add_argument('--output_dir', type=str, default=None, help="Directory to save metadata files")
    parser.add_argument('--html_output_dir', type=str, default=None, help="Directory to save HTML reports")
    parser.add_argument('--sensitive_columns_file', type=str, default=None, help="Path to file with sensitive column names")
    parser.add_argument('--no_hash', action='store_true', help="Skip sensitive data hashing")
    parser.add_argument('--log_file', type=str, help="Path to log file (if not specified, a timestamped log file is created automatically)")
    parser.add_argument('--log_level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help="Logging level (default: INFO)")
    parser.add_argument('--profile_type', type=str, default='full', choices=['full', 'minimal'], help="Profiling type: 'full' (default) or 'minimal' (faster, disables some features)")
    args = parser.parse_args()
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level=log_level, log_file=args.log_file)
    # Load env if provided
    if args.env_file:
        load_dotenv(args.env_file, override=True)
    # Sensitive columns
    sensitive_columns = None
    if args.sensitive_columns_file:
        sensitive_columns = get_sensitive_columns_from_file(args.sensitive_columns_file)
    # Default sensitive keywords
    sensitive_keywords = ["bvn", "id number", "nin", "passport", "driver", "identificationnumber", "chn", "email", "phone number", "mobile number", "address"]
    hash_sensitive = not args.no_hash
    html_output_dir = args.html_output_dir or "html_profile"
    output_dir = args.output_dir or "metadata_profile"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(html_output_dir, exist_ok=True)
    # Handle CSV profiling
    if args.csv:
        logging.info(f"Profiling CSV file: {args.csv}")
        df = generate_profiling_report(
            db_type='csv',
            path_to_csv=args.csv,
            html_output_dir=html_output_dir,
            sensitive_columns=sensitive_columns,
            sensitive_keywords=sensitive_keywords,
            hash_sensitive=hash_sensitive,
            profile_type=args.profile_type
        )
        if df.empty:
            logging.error("Failed to generate profiling report.")
            return 1
        generate_metadata_file(df, output_dir=output_dir)
    # Handle Oracle profiling
    elif args.db_type == 'oracle':
        if not args.schema:
            logging.error("--schema is required for Oracle DB")
            return 1
        if not args.tables_file:
            logging.error("--tables_file is required for Oracle DB")
            return 1
        # Load Oracle credentials from environment
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_sid = os.getenv('DB_SID')
        if not all([db_user, db_password, db_host, db_port, db_sid]):
            logging.error("Missing database credentials in environment variables. Required: DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_SID")
            return 1
        connection = connect_to_oracle(db_user, db_password, db_host, db_port, db_sid)
        if connection is None:
            logging.error("Failed to connect to Oracle. Cannot proceed.")
            return 1
        tables = get_list_of_tables(args.tables_file)
        if not tables:
            logging.error("No tables found in the specified file.")
            return 1
        df = generate_profiling_report(
            db_type='oracle',
            db_connection=connection,
            schema=args.schema,
            tables_list=tables,
            html_output_dir=html_output_dir,
            sensitive_columns=sensitive_columns,
            sensitive_keywords=sensitive_keywords,
            hash_sensitive=hash_sensitive,
            profile_type=args.profile_type
        )
        if df.empty:
            logging.error("Failed to generate profiling report.")
            return 1
        generate_metadata_file(df, output_dir=output_dir)
        connection.close()
    # Handle ClickHouse profiling
    elif args.db_type == 'clickhouse':
        # Load ClickHouse credentials from environment
        ch_host = os.getenv('host')
        ch_port = os.getenv('port')
        ch_user = os.getenv('user')
        ch_password = os.getenv('password')
        if not all([ch_host, ch_port, ch_user, ch_password]):
            logging.error("Missing ClickHouse credentials in environment variables. Required: host, port, user, password")
            return 1
        client = connect_to_clickhouse(ch_host, ch_port, ch_user, ch_password)
        if client is None:
            logging.error("Failed to connect to ClickHouse. Cannot proceed.")
            return 1
        if not args.tables_file:
            logging.error("--tables_file is required for ClickHouse DB")
            return 1
        tables = get_list_of_tables(args.tables_file)
        if not tables:
            logging.error("No tables found in the specified file.")
            return 1
        df = generate_profiling_report(
            db_type='clickhouse',
            db_connection=client,
            tables_list=tables,
            html_output_dir=html_output_dir,
            sensitive_columns=sensitive_columns,
            sensitive_keywords=sensitive_keywords,
            hash_sensitive=hash_sensitive,
            profile_type=args.profile_type
        )
        if df.empty:
            logging.error("Failed to generate profiling report.")
            return 1
        generate_metadata_file(df, output_dir=output_dir)
        try:
            client.disconnect()
        except Exception:
            pass
    logging.info("Process completed successfully!")
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)