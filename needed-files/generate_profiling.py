from ydata_profiling import ProfileReport
from ydata_profiling.config import Settings
import argparse
import pandas as pd
import hashlib
import json

# Merged function from both files
def read_json_ydata(ydata_json):
    # open file
    with open(ydata_json) as json_data:
        data = json.load(json_data)
        print(data.keys())
    
    # obtain values in the "variables" key and store in .csv file
    df = pd.DataFrame(data['variables'])
    df.transpose().to_csv(f'{ydata_json}.csv', index=True)
    
    print("Variables statistics have successfully been copied into .csv file")


def hash_column(column):
    """Hash the values in a column using SHA-256."""
    return column.apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notnull(x) else x)


def generate_profiling_report(path_to_csv, sensitive_columns=None):
    """
    Generate a profiling report (in html) using ydata-profiling
    
    Args:
        path_to_csv (str): path to csv 
        sensitive_columns (list): List of column names to mark as sensitive
        
    Returns:
        None
    """
    try:    
        file_name = str.split(path_to_csv, "/")[-1].split(".")[0]
        data = pd.read_csv(path_to_csv)
        
        # Automatically detect sensitive columns
        sensitive_keywords = ["bvn", "id number", "nin", "passport", "driver", "identificationnumber", "chn"]
        sensitive_columns = [
            col for col in data.columns 
            if any(keyword in col.lower() for keyword in sensitive_keywords)
        ]
        
        # Hash sensitive columns
        if sensitive_columns:
            for col in sensitive_columns:
                print(f"Hashing sensitive column: {col}")
                data[col] = hash_column(data[col])
        
        # Configure settings to mark sensitive columns
        config = Settings()
        if sensitive_columns:
            config.variables.descriptions = {col: "Sensitive Data (Hashed)" for col in sensitive_columns}
        
        # Generate profiling report    
        profile = ProfileReport(
            data,
            title=f"{file_name} Profiling Report",
            explorative=True,
            config=config)
        
        profile.to_file(f'{file_name}.html')
        json_data = profile.to_json()
        profile.to_file(f'{file_name}.json')
        read_json_ydata(f'{file_name}.json')
        
    except FileNotFoundError:
        print(f"Error: File not found at the path '{path_to_csv}'. Please check the path and try again.")
    except pd.errors.EmptyDataError:
        print(f"Error: The file at '{path_to_csv}' is empty.")
    except pd.errors.ParserError:
        print(f"Error: There was an issue parsing the file at '{path_to_csv}'. Is it a valid CSV?")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Generate ydata profiling report as html.")
    parser.add_argument(
        '--csv', 
        type=str, 
        required=True, 
        help="csv file to generate profiling report."
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Generate report
    generate_profiling_report(args.csv)
    
    # Get file name from the argument passed
    file_name = str.split(args.csv, "/")[-1].split(".")[0]
    
    print(f"Generated profiling report and saved to {file_name}.html.")


if __name__ == "__main__":
    main()

