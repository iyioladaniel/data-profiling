import pandas as pd
import random
from faker import Faker

fake = Faker()
Faker.seed(42)

# Define column mappings for realistic synthetic data
column_generators = {
    "CustomerBVN": lambda: str(random.randint(10000000000, 99999999999)),  # 11-digit BVN
    "Bvn": lambda: str(random.randint(10000000000, 99999999999)),  
    "userBVN": lambda: str(random.randint(10000000000, 99999999999)),  
    "CustAID": lambda: fake.uuid4(),
    "Customer ID": lambda: fake.uuid4(),
    "platformuserid": lambda: fake.uuid4(),
    "Account Officer": lambda: fake.name(),
    "Gender": lambda: random.choice(["Male", "Female"]),
    "IdentificationType": lambda: random.choice(["NIN", "Passport", "Driver License"]),
    "IdentificationNumber": lambda: str(random.randint(100000000, 999999999)),
    "IdentificationIssueDate": lambda: fake.date_between(start_date="-10y", end_date="-1y"),
    "IdentificationExpiryDate": lambda: fake.date_between(start_date="today", end_date="+10y"),
    "RMID": lambda: fake.uuid4(),
    "Occupation": lambda: fake.job(),
    "Country": lambda: fake.country(),
    "Passport Number": lambda: fake.passport_number(),
    "Driver Licence": lambda: fake.license_plate(),
    "Marital Status": lambda: random.choice(["Single", "Married", "Divorced"]),
    "Postal Code": lambda: fake.postcode(),
    "Acctno": lambda: fake.uuid4(),
    "dob": lambda: fake.date_of_birth(minimum_age=18, maximum_age=80),
    "phone": lambda: fake.phone_number(),
    "email_verif": lambda: random.choice([True, False]),
    "nextofkin": lambda: fake.name(),
    "status": lambda: random.choice(["Active", "Inactive"]),
    "spec_tax_rate": lambda: round(random.uniform(5.0, 20.0), 2),
    "curr_code": lambda: random.choice(["USD", "NGN", "GBP"]),
}

# List of file structures
file_definitions = {
    "asset_custdata.csv": ["CustAID", "CustomerBVN", "ID Number", "Gender", "Occupation", "Account Officer"],
    "digital_trustees_custdata.csv": ["platformuserid", "Bvn", "IdentificationType", "IdentificationNumber", 
                                      "IdentificationIssueDate", "IdentificationExpiryDate", "Gender", "RMID", "IsPep", "CustomerId"],
    "individual_insurance_custdata.csv": ["Customer ID", "Country of Birth", "Gender", "NIN", "Passport Number", 
                                          "Country", "Occupation", "Driver Licence", "Marital Status", "Postal Code"],
    "registrars_shold.csv": ["Acctno", "regcode", "agent", "title", "sex", "dob", "st", "typer", "phone", "narr", 
                             "oldaccountno", "chn", "maiden", "active", "mand_acct", "status1", "status", "keep", 
                             "narration1", "chn_acct", "nextofkin", "deceased", "consolid_id", "occupation_code", 
                             "caution_ref", "fbn_code", "auto", "chn_member", "bvn", "caution_dt", "email_verif", 
                             "spec_tax_rt", "spec_tax_rate", "gvn", "curr_code"],
    "securities_custdata.csv": ["CustAID", "CustomerBVN", "ID Number", "MaritalStat", "Occupation", "AccountOfficer"],
    "traditional_trustees_custdata.csv": ["CustAID", "CustomerBVN", "ID Number", "Gender", "Occupation", "Account Officer"],
}

# Generate synthetic data for each file
num_records = 100  # Adjust as needed

for file_name, columns in file_definitions.items():
    data = {col: [column_generators.get(col, lambda: fake.word())() for _ in range(num_records)] for col in columns}
    
    df = pd.DataFrame(data)
    df.to_csv(file_name, index=False)
    print(f"Generated {num_records} records for {file_name}")

print("✅ Synthetic data generation complete!")

