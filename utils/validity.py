import pandas as pd
import re
import sys

# Predefined regex patterns for Nigerian identity codes
ID_PATTERNS = {
    "nin": r"^\d{11}$",  # National Identification Number (11 digits)
    "bvn": r"^\d{11}$",  # Bank Verification Number (11 digits)
    "passport": r"^[A-Z]\d{8}$",  # Nigerian Passport Number (1 letter followed by 8 digits)
    "driverslicense": r"^[A-Z]{2}\d{6}[A-Z]{1}$"  # Nigerian Driver's License
}

# Column name aliases for matching
COLUMN_ALIASES = {
    "nin": ["nin"],
    "bvn": ["bvn"],
    "passport": ["passport", "passportnumber"],
    "driverslicense": ["driverslicense", "driverlicence", "driverslicence", "driverlicense"]
}

def preprocess_column_name(column_name):
    """
    Preprocess a column name by removing spaces, converting to lowercase,
    and keeping only alphabetic characters.

    Args:
        column_name (str): Original column name.

    Returns:
        str: Preprocessed column name.
    """
    column_name = column_name.replace(" ", "").lower()
    column_name = re.sub(r"[^a-z]", "", column_name)  # Remove non-alphabetic characters
    return column_name

def match_columns(columns):
    """
    Match the preprocessed column names to known ID types.

    Args:
        columns (list): List of column names.

    Returns:
        dict: Matched columns with their corresponding ID type.
    """
    matched_columns = {}
    for column in columns:
        for id_type, aliases in COLUMN_ALIASES.items():
            if any(alias in column for alias in aliases):
                matched_columns[column] = id_type
                break
    return matched_columns

def validate_columns(file_path):
    """
    Validate columns in a CSV file that match known ID types.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        None
    """
    try:
        # Load the CSV file
        data = pd.read_csv(file_path)

        # Preprocess column names
        preprocessed_columns = [preprocess_column_name(col) for col in data.columns]

        # Match columns to ID types
        matched_columns = match_columns(preprocessed_columns)

        if not matched_columns:
            print("No matching columns found for validation.")
            return

        print(f"Matched columns: {matched_columns}")

        # Validate each matched column
        for column, id_type in matched_columns.items():
            regex_pattern = ID_PATTERNS[id_type]
            pattern = re.compile(regex_pattern)

            # Add a validation column
            data[f"{column}_valid"] = data[column].astype(str).apply(lambda x: bool(pattern.fullmatch(x)))

            # Count valid and invalid entries
            valid_count = data[f"{column}_valid"].sum()
            invalid_count = len(data) - valid_count
            valid_percent = round((valid_count / len(data)) * 100, 2)

            print(f"Validation for column '{column}' ({id_type}):")
            print(f"  Valid entries: {valid_count}")
            print(f"  Invalid entries: {invalid_count}")
            print(f"  Validity Percentage: {valid_percent}%\n")

        # Save results to a new file
        output_file = f"{file_path}_validation_results.csv"
        data.to_csv(output_file, index=False)
        print(f"Validation results saved to '{output_file}'.")

    except FileNotFoundError:
        print(f"Error: File not found at path '{file_path}'. Please check the file path and try again.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Check if the file path argument is provided
    if len(sys.argv) < 2:
        print("Usage: python3 validate_ids.py <path_to_csv>")
        sys.exit(1)

    # Get the CSV file path from the command line arguments
    csv_file_path = sys.argv[1]

    # Call the validation function
    validate_columns(csv_file_path)
