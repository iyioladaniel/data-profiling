import pandas as pd
import re
import sys

# Predefined regex patterns for Nigerian identity codes
ID_PATTERNS = {
    "nin": r"^\d{11}(\.0)?$",  # National Identification Number (11 digits) with optional ".0" in cases where the number is saved as a float
    "bvn": r"^22\d{9}(\.0)?$",  # Bank Verification Number (11 digits) with optional ".0" in cases where the number is saved as a float
    "passport": r"^[A-Z]\d{8}$",  # Nigerian Passport Number (1 letter followed by 8 digits)
    "driverslicense": r"^[A-Z]{3}\d{5}[A-Z]{2}\d{1,2}$"  # Nigerian Driver's License (FRN123456AB12 format)
}

# Column name aliases for matching
COLUMN_ALIASES = {
    "nin": ["nin", "nationalid", "nationalidentification"],
    "bvn": ["bvn", "bankverification", "bankverificationnumber", "customerbvn"],
    "passport": ["passport", "passportnumber", "internationalpassport"],
    "driverslicense": ["driverslicense", "driverlicence", "driverslicence", "driverlicense", "dlicense"]
}

def clean_id_value(value, id_type):
    """
    Clean ID values by removing spaces and standardizing format.
    
    Args:
        value (str): Original ID value.
        id_type (str): Type of ID being cleaned.
    
    Returns:
        str: Cleaned ID value.
    """
    if pd.isna(value):
        return ""
    
    # Convert to string and remove spaces
    value = str(value).strip().replace(" ", "").upper()
    
    if id_type in ["bvn", "nin"]:
        # For BVN and NIN, we only remove special characters except decimal point
        return value
    else:
        # For other IDs, remove all special characters
        return re.sub(r'[^A-Z0-9]', '', value)

def preprocess_column_name(column_name):
    """
    Preprocess a column name by removing spaces, converting to lowercase,
    and keeping only alphabetic characters.
    """
    column_name = str(column_name).replace(" ", "").lower()
    column_name = re.sub(r"[^a-z]", "", column_name)
    return column_name

def match_columns(columns):
    """
    Match the preprocessed column names to known ID types.
    """
    matched_columns = {}
    preprocessed_columns = {preprocess_column_name(col): col for col in columns}
    
    for preprocessed_col, original_col in preprocessed_columns.items():
        for id_type, aliases in COLUMN_ALIASES.items():
            if any(alias in preprocessed_col for alias in aliases):
                matched_columns[original_col] = id_type
                break
    return matched_columns

def validate_id(value, id_type):
    """
    Validate a single ID value against its expected pattern.
    """
    if pd.isna(value) or value == "":
        return False
        
    value = clean_id_value(value, id_type)
    pattern = re.compile(ID_PATTERNS[id_type])
    
    return bool(pattern.fullmatch(value))

def validate_columns(file_path):
    """
    Validate columns in a CSV file that match known ID types.
    """
    try:
        # Load the CSV file
        data = pd.read_csv(file_path)

        # Match columns to ID types
        matched_columns = match_columns(data.columns)

        if not matched_columns:
            print("No matching columns found for validation.")
            return

        # Create a dictionary to save the results
        validity_dict = {
            "file":[],
            "column": [],
            "valid_entries": [],
            "invalid_entries": [],
            "validity_percent": []
        }
        
        # Get file name
        file_name = str.split(file_path, "/")[-1].split(".")[0]
        
        # Validate each matched column
        for column, id_type in matched_columns.items():
            # Create validation column
            data[f"{column}_valid"] = data[column].apply(lambda x: validate_id(x, id_type))

            # Calculate statistics
            valid_count = data[f"{column}_valid"].sum()
            invalid_count = len(data) - valid_count
            valid_percent = round((valid_count / len(data)) * 100, 2)

            print(f"\nValidation Summary for '{column}' ({id_type}):")
            print(f"Valid entries: {valid_count}")
            print(f"Invalid entries: {invalid_count}")
            print(f"Validity Percentage: {valid_percent}%")
            
            # Add records to validity dictionary
            validity_dict["file"].append(file_name)
            validity_dict["column"].append(column)
            validity_dict["valid_entries"].append(valid_count)
            validity_dict["invalid_entries"].append(invalid_count)
            validity_dict["validity_percent"].append(valid_percent)

        # Create validity dataframe and output the results to a file
        validity_df = pd.DataFrame(validity_dict)
        output_file = './validation/' + str.split(file_path, "/")[-1].split(".")[0] + '_validation_results.csv'
        validity_df.to_csv(output_file, index=False)
        print(f"\nFull validation results saved to '{output_file}'")
        
        # # Save results to a new file
        # output_file = file_path.rsplit('.', 1)[0] + '_validation_results.csv'
        # data.to_csv(output_file, index=False)
        # print(f"\nFull validation results saved to '{output_file}'")

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
