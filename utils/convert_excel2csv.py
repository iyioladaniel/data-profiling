import sys
import os
import pandas as pd

def convert_excel_to_csv(file_path):
    """
    Converts all sheets in an Excel file to individual CSV files.

    Args:
        file_path (str): The path to the Excel file.

    Returns:
        None
    """
    try:
        # Verify the file is an Excel file
        if not (file_path.endswith(".xls") or file_path.endswith(".xlsx")):
            print("Error: The provided file is not an Excel file.")
            return
        
        # Load the Excel file
        excel_file = pd.ExcelFile(file_path)
        
        # Get the number of sheets
        sheet_names = excel_file.sheet_names
        print(f"Excel file '{file_path}' contains {len(sheet_names)} sheet(s): {sheet_names}")
        
        # Create an output directory for CSVs
        output_dir = os.path.splitext(file_path)[0] + "_csvs"
        os.makedirs(output_dir, exist_ok=True)
        print(f"CSV files will be saved in the directory: {output_dir}")
        
        # Get file name
        file_name = str.split(file_path, "/")[-1].split(".")[0]
        
        # Convert each sheet to a CSV file
        for sheet_name in sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            csv_file_path = os.path.join(output_dir, f"{file_name}_{sheet_name}.csv")
            df.to_csv(csv_file_path, index=False)
            print(f"Converted sheet '{sheet_name}' to CSV: {csv_file_path}")
        
        print("All sheets have been successfully converted to CSV files.")
    
    except FileNotFoundError:
        print(f"Error: File not found at path '{file_path}'. Please check the file path and try again.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Check if a file path is provided
    if len(sys.argv) != 2:
        print("Usage: python3 excel_to_csv.py <path_to_excel_file>")
        sys.exit(1)
    
    # Get the file path from the command-line argument
    excel_file_path = sys.argv[1]
    
    # Call the function to convert the Excel file
    convert_excel_to_csv(excel_file_path)
