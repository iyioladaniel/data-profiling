import logging
import pandas as pd
from faker import Faker
import random
import string
from ydata_profiling import ProfileReport

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

#instantiating the faker object
fake = Faker()

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
        return number
    
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
    Format: ^[A-Z]-[A-Z]{5}[0-9]{2}-[0-9]{7}$
    
    Args:
        null_probability (float): Probability of generating None instead of an ID
    
    Returns:
        str or None: A driver's license number or None
    """
    if random.random() < null_probability:
        return None
        
    first_letter = random.choice(string.ascii_uppercase)
    middle_letters = ''.join(random.choices(string.ascii_uppercase, k=5))
    middle_numbers = ''.join(str(random.randint(0, 9)) for _ in range(2))
    last_numbers = ''.join(str(random.randint(0, 9)) for _ in range(7))
    
    return f"{first_letter}-{middle_letters}{middle_numbers}-{last_numbers}"

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
        data['nin'].append(generate_11_digit_number(null_probability=0.4))
        data['passport'].append(generate_nigerian_passport_id(null_probability=0.5))
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

# trial_df = generate_customer_data(5)
# trial_df
# profile = ProfileReport(trial_df, title="Trial Profiling Report")
# profile.to_file('trial_report.html')