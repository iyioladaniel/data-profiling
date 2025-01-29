# Import necessary modules
import random  # For generating random choices
from faker import Faker  # For generating fake data
import pandas as pd  # For creating and managing DataFrames
import argparse  # For parsing command-line arguments
import string

def generate_11_digit_number(null_probability=0.2):
    """
    Generate a random 11-digit number using Faker, can be null depending on probability
    
    Args:
        null_probability (float): Probability of generating a null value (between 0 and 1)
        
    Returns:
        int: a single 11-digit number as int or None
    """
    fake = Faker()
    # Randomly decide whether to generate a null value
    if random.random() < null_probability:
        return None
    else:
        # Generate number between 10000000000 and 99999999999
        number = fake.random_number(digits=11, fix_len=True)
        return int(number)
    
def generate_nigerian_passport_id(null_probability=0.2):
    """
    Generate random Nigerian passport IDs matching pattern ^[A-Z]{1,2}[0-9]{7}$
    
    Args:
        null_probability (float): Probability of generating a null value (between 0 and 1)
        
    Returns:
        str: single passport ID as strings or None
    """
    fake = Faker()
    
    if random.random() < null_probability:
        return None
    else:
        # Randomly decide between 1 or 2 letters
        letter_count = random.randint(1, 2)
        # Generate random uppercase letters
        letters = ''.join(random.choices(string.ascii_uppercase, k=letter_count))
        # Generate 7 random digits
        numbers = ''.join(str(random.randint(0, 9)) for _ in range(7))
        passport_id = f"{letters}{numbers}"
        return passport_id
    
def generate_voters_card(null_probability=0.2):
    """
    Generate a single voter's card number matching pattern AAA/BBB/CC/DDDDD
    Format: ^[A-Z]{3}/\d{3}/\d{2}/\d{5}$
    
    Args:
        null_probability (float): Probability of generating None instead of an ID
    
    Returns:
        str or None: A voter's card number or None
    """
    if random.random() < null_probability:
        return None
        
    first_part = ''.join(random.choices(string.ascii_uppercase, k=3))
    second_part = ''.join(str(random.randint(0, 9)) for _ in range(3))
    third_part = ''.join(str(random.randint(0, 9)) for _ in range(2))
    fourth_part = ''.join(str(random.randint(0, 9)) for _ in range(5))
    
    return f"{first_part}/{second_part}/{third_part}/{fourth_part}"

def generate_tax_id(null_probability=0.2):
    """
    Generate a single tax ID (TIN) matching pattern DDDDDDDDDD
    Format: ^\d{10}$
    
    Args:
        null_probability (float): Probability of generating None instead of an ID
    
    Returns:
        str or None: A tax ID number or None
    """
    if random.random() < null_probability:
        return None
        
    return ''.join(str(random.randint(0, 9)) for _ in range(10))

def generate_cac_number(null_probability=0.2):
    """
    Generate a single CAC number matching pattern RC/NNNNNNN or BN/NNNNNNN
    Format: ^(RC|BN)/\d{7}$
    
    Args:
        null_probability (float): Probability of generating None instead of an ID
    
    Returns:
        str or None: A CAC registration number or None
    """
    if random.random() < null_probability:
        return None
        
    prefix = random.choice(['RC', 'BN'])
    numbers = ''.join(str(random.randint(0, 9)) for _ in range(7))
    
    return f"{prefix}/{numbers}"

def generate_drivers_license(null_probability=0.2):
    """
    Generate a single driver's license number matching pattern A-BBBBBCC-DDDDDDD
    Format: ^[A-Z]{3}[0-9]{5}[A-Z]{2}[0-9]{1,2}$
    
    Args:
        null_probability (float): Probability of generating None instead of an ID
    
    Returns:
        str or None: A driver's license number or None
    """
    if random.random() < null_probability:
        return None
        
    #first_letter = random.choice(string.ascii_uppercase)
    first_letters = ''.join(random.choices(string.ascii_uppercase, k=3))
    middle_numbers = ''.join(str(random.randint(0, 9)) for _ in range(5))
    middle_letters = ''.join(random.choices(string.ascii_uppercase, k=2))
    last_numbers = ''.join(str(random.randint(0, 9)) for _ in range(1))
    
    return f"{first_letters}{middle_numbers}{middle_letters}{last_numbers}"

def generate_customer_data(count=1):
    """
    Generate a DataFrame with random customer data
    
    Args:
        count (int): Number of customer records to generate
        
    Returns:
        pandas.DataFrame: DataFrame containing customer records
    """
    fake = Faker()
    
    # Initialize empty lists for each column
    data = {
        'customer_id': [],
        'occupation': [],
        'nin': [],
        'passport': [],
        'country_of_birth': [],
        'marital_status': [],
        'drivers_license': [],
        'voters_card': [],
        'tax_id': [],
        'cac_number': [],
        'gender': [],
        'postal_code': []
    }
    
    # Generate data for each row
    for _ in range(count):
        data['customer_id'].append(str(fake.random_number(digits=11, fix_len=True)))
        data['occupation'].append(fake.job())
        data['nin'].append(generate_11_digit_number(null_probability=0.3))
        data['passport'].append(generate_nigerian_passport_id(null_probability=0.2))
        data['country_of_birth'].append(fake.country())
        data['marital_status'].append(random.choice(["Single", "Married", "Widowed"]))
        data['drivers_license'].append(generate_drivers_license())
        data['voters_card'].append(generate_voters_card())
        data['tax_id'].append(generate_tax_id())
        data['cac_number'].append(generate_cac_number())
        data['gender'].append(fake.random_element(elements=('M', 'F')))
        data['postal_code'].append(fake.postalcode())
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    return df

# # Function to generate a DataFrame with customer data
# def generate_customer_data(count):
#     """
#     Generate a DataFrame with customer data.
    
#     Args:
#         count (int): Number of records to generate.
        
#     Returns:
#         pandas.DataFrame: DataFrame containing customer data.
#     """
#     # Create an instance of Faker to generate fake data
#     fake = Faker()
    
#     # Initialize a dictionary to store data for each column
#     data = {
#         'customer_id': [],        # Unique ID for each customer
#         'Country of Birth': [],   # Random country of birth
#         'Gender': [],             # Gender (Male or Female)
#         'NIN': [],                # National Identification Number (random pattern)
#         'Passport Number': [],    # Randomly generated passport number
#         'Country': [],            # Random country of residence
#         'Occupation': [],         # Random job/occupation
#         'Driver Licence': [],     # Randomly generated driver's license number
#         'Marital Status': [],     # Marital status (e.g., Single, Married)
#         'Postal Code': []         # Random postal code for the customer's address
#     }
    
#     # List of possible genders
#     genders = ['Male', 'Female']
#     # List of possible marital statuses
#     marital_statuses = ['Single', 'Married']
    
#     # Loop to generate data for the specified number of records
#     for i in range(count):
#         # Assign a unique customer ID (starting from 1)
#         data['customer_id'].append(i + 1)
#         # Generate a random country of birth
#         data['Country of Birth'].append(fake.country())
#         # Randomly select a gender
#         data['Gender'].append(random.choice(genders))
#         # Generate a random National Identification Number (e.g., AB1234567)
#         data['NIN'].append(fake.bothify('??#######'))
#         # Generate a random passport number (e.g., P12345678)
#         data['Passport Number'].append(fake.bothify('P########'))
#         # Generate a random country of residence
#         data['Country'].append(fake.country())
#         # Generate a random job/occupation
#         data['Occupation'].append(fake.job())
#         # Generate a random driver's license number (e.g., DL12345678)
#         data['Driver Licence'].append(fake.bothify('DL########'))
#         # Randomly select a marital status
#         data['Marital Status'].append(random.choice(marital_statuses))
#         # Generate a random postal code
#         data['Postal Code'].append(fake.postcode())
    
#     # Create a DataFrame from the generated data
#     df = pd.DataFrame(data)
    
#     # Return the resulting DataFrame
#     return df

# Main function to handle command-line arguments and generate/save customer data
def main():
    # Set up an argument parser to allow the user to specify the number of records to generate
    parser = argparse.ArgumentParser(description="Generate customer data and save to CSV.")
    parser.add_argument(
        '--count',  # Command-line argument for specifying the record count
        type=int,  # The value of the argument must be an integer
        required=True,  # This argument is mandatory
        help="Number of records to generate."  # Help message for the argument
    )
    
    # Parse command-line arguments provided by the user
    args = parser.parse_args()
    
    # Generate the specified number of customer records
    df = generate_customer_data(args.count)
    
    # Specify the name of the output CSV file
    output_file = "insurance_individual_customer_data.csv"
    # Save the generated DataFrame to a CSV file in the current directory
    df.to_csv(output_file, index=False)
    # Print a success message with details of the generated file
    print(f"Generated {args.count} records and saved to {output_file}.")

# Run the main function when the script is executed directly
if __name__ == "__main__":
    main()
