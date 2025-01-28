import string
import random
import argparse
import pandas as pd



hold_type = [i for i in string.ascii_uppercase]

categories = ["INDIVIDUAL","BANK","CORPORATE BODY","DECEASED","RELIGIOUS BODIES","FEDERAL GOVERNMENT","STATE GOVERNMENT","AMCON","INSURANCE","MUTUAL FUNDS","DIRECTORS","LOCAL GOVERNMENT","FEDERAL INSTITUTIONS","NOMINEES","COOPERATIVES","PENSION FUND","PEP","INSTITUTIONAL","REGISTRARS","STAFF","STOCKBROKING FIRMS","SCHOOLS / INSTITUTIONS","JOINT HOLDERS","HNI","FUND MANAGER","FOREIGN INVESTORS"]

def generate_shareholder_type_data(count):
    """
    Generate a DataFrame of shareholder details
    
    Args:
        count (int): Number of customer records to generate
        
    Returns:
        pandas.DataFrame: DataFrame containing shareholder details
    """
        
    # Initialize empty lists for each column
    data = {
        'acctno': hold_type,
        'regcode': categories,
        'agent': [],
        'title': [],
        'sex': [],
        'dob': [],
        'st': [],
        'typer': [],
        'phone': [],
        'narr': [],
        'oldaccountno': [],
        'chn': [],
        
    }
    # Create DataFrame
    df = pd.DataFrame(data)
    
    return df

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Generate shareholder type data and save to CSV.")
    parser.add_argument(
        '--count', 
        type=int, 
        required=True, 
        help="Number of shareholder type records to generate."
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Generate data
    df = generate_shareholder_type_data(args.count)
    
    # Save to CSV in the current directory
    output_file = "shareholder_type_data.csv"
    df.to_csv(output_file, index=False)
    print(f"Generated {args.count} records and saved to {output_file}.")

if __name__ == "__main__":
    main()