from ydata_profiling import ProfileReport
import argparse
import pandas as pd
import json
#import string

def read_json_ydata(ydata_json):
    # open file
    with open(ydata_json) as json_data:
        data = json.load(json_data)
        print(data.keys())
    
    # obtain values in the "vaariables" key and store in .csv file
    df = pd.DataFrame(data['variables'])
    df.transpose().to_csv(f'{ydata_json}.csv', index=True)
    
    print("Variables statistics have successfuly been copied into .csv file")


def generate_profiling_report(path_to_csv):
    """
    Generate an profiling report (in html) using ydata-profiling
    
    Args:
        path_to_csv (str): path to csv 
        
    Returns:
        int: a single 11-digit number as int or None
    """
    try:    
        file_name = str.split(path_to_csv, "/")[-1].split(".")[0]
        data = pd.read_csv(path_to_csv)    
        profile = ProfileReport(data, title=f"{file_name} Profiling Report", explorative=True)
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
    
    #Get file name from the argument passed
    file_name = str.split(args.csv, "/")[-1].split(".")[0]
    
    print(f"Generated profiling report and saved to {file_name}.html.")

if __name__ == "__main__":
    main()


