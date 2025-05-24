# Data Profile to Metadata Generator

*Last updated: May 24, 2025*

## Overview

This script (`profile-to-metadata.py`) generates comprehensive data profiling reports and converts them into metadata repositories. It analyzes data structures from CSV files, Oracle, or ClickHouse database tables and extracts key metrics including:

- Data types
- Completeness metrics (missing values)
- Cardinality (unique values)
- Value distributions
- Statistical properties
- Schema information
- Sensitive data detection

The tool automatically identifies and securely hashes sensitive data fields to protect PII (Personally Identifiable Information).

## Requirements

- Python 3.8+ 
- Dependencies (install using `pip install -r requirements.txt`):
  - ydata-profiling
  - pandas
  - oracledb
  - clickhouse-driver
  - python-dotenv
  - hashlib

## Quick Start

### Profiling a CSV File

```bash
python profile-to-metadata.py --csv /path/to/your/file.csv
```

### Profiling Oracle Database Tables

```bash
python profile-to-metadata.py --db_type oracle --schema YOUR_SCHEMA --tables_file tables.txt --env_file /path/to/oracle.env
```

### Profiling ClickHouse Database Tables

```bash
python profile-to-metadata.py --db_type clickhouse --tables_file tables.txt --env_file /path/to/clickhouse.env
```

## Command-Line Options

### Source Options (Required - Choose One)

| Option | Description |
|--------|-------------|
| `--csv PATH` | Path to CSV file to profile |
| `--db_type {oracle,clickhouse}` | Use Oracle or ClickHouse database connection instead of CSV |

### Database Options (Required with --db_type)

| Option | Description |
|--------|-------------|
| `--schema SCHEMA` | Oracle schema name (Oracle only) |
| `--tables_file PATH` | Path to file listing tables to profile (one per line) |
| `--env_file PATH` | Path to .env file with database credentials (default: .env) |
| `--thick` | Use Oracle thick mode client instead of thin mode (Oracle only) |

### Additional Options

| Option | Description |
|--------|-------------|
| `--output_dir PATH` | Directory to save metadata files (default: metadata_profile) |
| `--html_output_dir PATH` | Directory to save HTML reports (default: html_profile) |
| `--sensitive_columns_file PATH` | Path to file with sensitive column names |
| `--no_hash` | Skip sensitive data hashing |
| `--log_file PATH` | Custom path to log file (default: auto-generated in current directory) |
| `--log_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}` | Logging level (default: INFO) |
| `--profile_type {full,minimal}` | Profiling type: 'full' (default, detailed) or 'minimal' (faster, disables some features) |

## Profiling Modes

- **full** (default): Generates a comprehensive profiling report with all features enabled (correlations, samples, missing diagrams, interactions, etc.).
- **minimal**: Generates a faster, lightweight report with correlations, samples, missing diagrams, and interactions disabled. Date columns are inferred and passed as type schema. Use this for large datasets or quick scans.

## Environment File Format for Oracle Connection

Create a `.env` file with the following structure:

```
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=1521
DB_SID=your_sid
```

## Environment File Format for ClickHouse Connection

Create a `.env` file with the following structure:

```
host=your_clickhouse_host
port=9000
user=your_clickhouse_user
password=your_clickhouse_password
```

## Tables File Format

Create a text file with one table name per line:
```
TABLE1
TABLE2
TABLE3
```

## Output Files

The script generates:

1. A CSV metadata file containing all profiled columns with statistics
2. A timestamped log file with processing details
3. An HTML profiling report for each table or file

## Troubleshooting

### Common Issues

1. **Missing Database Credentials**
   - Check that your `.env` file exists and contains all required variables
   - Use `--env_file` to specify a custom environment file location

2. **Oracle/ClickHouse Connection Issues**
   - Verify network connectivity to the database server
   - For Oracle, try using `--thick` option if thin mode fails
   - Check logs for specific error codes and messages

3. **Empty Output Files**
   - Set `--log_level DEBUG` for more detailed information
   - Check if input tables or CSV files exist and are accessible
   - Verify the script has write permissions in the output directory

### Debugging

For detailed troubleshooting:

1. Run with increased verbosity:
   ```
   python profile-to-metadata.py --csv data.csv --log_level DEBUG
   ```

2. Check the automatically generated log file in the current directory or specify your own:
   ```
   python profile-to-metadata.py --csv data.csv --log_file debug.log
   ```

3. Examine the log file for errors or warnings

## Example Workflows

### Basic CSV Profiling (Full)

```bash
python profile-to-metadata.py --csv data/raw/customer_data.csv
```

### Minimal Profiling (CSV)

```bash
python profile-to-metadata.py --csv data/raw/customer_data.csv --profile_type minimal
```

### Database Profiling with Custom Output (Full, Oracle)

```bash
python profile-to-metadata.py --db_type oracle --schema CUSTOMER --tables_file tables.txt --output_dir reports/metadata --profile_type full
```

### Database Profiling with Minimal Report (Oracle)

```bash
python profile-to-metadata.py --db_type oracle --schema CUSTOMER --tables_file tables.txt --profile_type minimal
```

### ClickHouse Profiling (Full)

```bash
python profile-to-metadata.py --db_type clickhouse --tables_file tables.txt --env_file clickhouse.env --profile_type full
```

### ClickHouse Profiling (Minimal)

```bash
python profile-to-metadata.py --db_type clickhouse --tables_file tables.txt --env_file clickhouse.env --profile_type minimal
```

## How It Works

1. The script reads data from the specified source (CSV, Oracle, or ClickHouse DB)
2. It identifies potential sensitive data columns using predefined keywords or a provided file
3. It hashes any sensitive data to protect privacy (unless `--no_hash` is set)
4. It generates a profiling report using YData Profiling (HTML and JSON)
5. It extracts key metrics and enriches with database metadata (if available)
6. It outputs a consolidated metadata file in CSV format

## Contributing

To contribute to this project:

1. Follow the code style and documentation patterns
2. Add tests for new features
3. Update this README with any changes to functionality