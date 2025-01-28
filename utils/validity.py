import pandas as pd
import re
import sys

def validate_column(file_path, column_name, regex_pattern):
    """
    Validates the entries in a specific column of a CSV file against a regex pattern.

    Args:
        file_path (str): Path to the CSV file.
        column_name (str): Name of the column to validate.
        regex_pattern (str): The regex pattern to validate the column against.

    Returns:
        None
    """
    try:
        # Load the CSV file into a DataFrame
        data = pd.read_csv(file_path)

        # Check if the column exists in the CSV file
        if column_name not in data.columns:
            print(f"Error: Column '{column_name}' not found in the file.")
            return

        # Compile the regex pattern
        pattern = re.compile(regex_pattern)

        # Validate each value in the column
        data["Valid"] = data[column_name].astype(str).apply(lambda x: bool(pattern.fullmatch(x)))

        # Count valid and invalid entries
        valid_count = data["Valid"].sum()
        invalid_count = len(data) - valid_count
        valid_percent = round((valid_count/len(data))*100, 2)

        # Save results to a new file
        output_file = "validation_results.csv"
        data.to_csv(output_file, index=False)

        print(f"Validation complete. Results saved to '{output_file}'.")
        print(f"Valid entries: {valid_count}")
        print(f"Invalid entries: {invalid_count}")
        print(f"Validity Percentage: {valid_percent}%")
    
    except FileNotFoundError:
        print(f"Error: File not found at path '{file_path}'. Please check the file path and try again.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Check if all arguments are provided
    if len(sys.argv) != 4:
        print("Usage: python3 validate_column.py <path_to_csv> <column_name> <regex_pattern>")
        sys.exit(1)
    
    # Get arguments from the command line
    csv_file_path = sys.argv[1]
    column_name = sys.argv[2]
    regex = sys.argv[3]
    
    # Call the validation function
    validate_column(csv_file_path, column_name, regex)
