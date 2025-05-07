# Data Profile to Metadata Generator

*Last updated: May 7, 2025*

## Overview

This script (`profile-to-metadata.py`) generates comprehensive data profiling reports and converts them into metadata repositories. It analyzes data structures from CSV files or Oracle database tables and extracts key metrics including:

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
  - python-dotenv
  - hashlib

## Quick Start

### Profiling a CSV File

```bash
python profile-to-metadata.py --csv /path/to/your/file.csv
```

### Profiling Oracle Database Tables

```bash
python profile-to-metadata.py --db --schema YOUR_SCHEMA --tables_file tables.txt
```

## Command-Line Options

### Source Options (Required - Choose One)

| Option | Description |
|--------|-------------|
| `--csv PATH` | Path to CSV file to profile |
| `--db` | Use Oracle database connection instead of CSV |

### Database Options (Required with --db)

| Option | Description |
|--------|-------------|
| `--schema SCHEMA` | Oracle schema name |
| `--tables_file PATH` | Path to file listing tables to profile (one per line) |
| `--env_file PATH` | Path to .env file with database credentials (default: .env) |
| `--thick` | Use Oracle thick mode client instead of thin mode |

### Additional Options

| Option | Description |
|--------|-------------|
| `--output PATH` | Custom path to save metadata file (default: based on input source) |
| `--no_hash` | Skip sensitive data hashing |
| `--log_file PATH` | Custom path to log file (default: auto-generated in current directory) |
| `--log_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}` | Logging level (default: INFO) |

## Environment File Format for Oracle Connection

Create a `.env` file with the following structure:

```
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=1521
DB_SID=your_sid
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

## Troubleshooting

### Common Issues

1. **Missing Database Credentials**
   - Check that your `.env` file exists and contains all required variables
   - Use `--env_file` to specify a custom environment file location

2. **Oracle Connection Issues**
   - Verify network connectivity to the database server
   - Try using `--thick` option if thin mode fails
   - Check logs for specific Oracle error codes and messages

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

### Basic CSV Profiling

```bash
python profile-to-metadata.py --csv data/raw/customer_data.csv
```

### Database Profiling with Custom Output

```bash
python profile-to-metadata.py --db --schema CUSTOMER --tables_file tables.txt --output reports/customer_metadata.csv
```

### Extended Debugging Session

```bash
python profile-to-metadata.py --csv data.csv --log_level DEBUG --log_file logs/detailed_debug.log
```

## How It Works

1. The script reads data from the specified source (CSV or Oracle DB)
2. It identifies potential sensitive data columns using predefined keywords
3. It hashes any sensitive data to protect privacy
4. It generates a comprehensive profiling report using YData Profiling
5. It extracts key metrics and enriches with database metadata (if available)
6. It outputs a consolidated metadata file in CSV format

## Contributing

To contribute to this project:

1. Follow the code style and documentation patterns
2. Add tests for new features
3. Update this README with any changes to functionality