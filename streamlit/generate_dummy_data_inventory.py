import pandas as pd
import random

# Define possible values for each column
companies = ["Insurance General", "Insurance Life", "Trustees", "Securities", "Asset Management"]
domains = ["Customer", "Product", "Transaction"]
table_names = {
    "Customer": ["Customer_Details", "Customer_Accounts"],
    "Product": ["Product_List", "Fund_List"],
    "Transaction": ["Transactions", "Audit_Log"]
}
field_descriptions = {
    "Customer": ["Customer_ID", "First_Name", "Last_Name", "Email", "Phone"],
    "Product": ["Product_ID", "Product_Name", "Product_Type", "Fund_ID", "Fund_Name"],
    "Transaction": ["Transaction_ID", "Amount", "Date", "Account_Number", "Status"]
}
data_types = ["Integer", "String", "Decimal", "Date"]
nullable = ["Yes", "No"]

# Generate data
data = []
for _ in range(1000):
    company = random.choice(companies)
    domain = random.choice(domains)
    table_name = random.choice(table_names[domain])
    field_name = random.choice(field_descriptions[domain])
    field_description = f"Description for {field_name}"
    data_type = random.choice(data_types)
    format_pattern = "" if data_type != "Date" else "YYYY-MM-DD"
    field_length = random.randint(10, 100)
    default_value = ""
    nullable_value = random.choice(nullable)
    source_system = f"{company.replace(' ', '_')}_System"
    relationship_mapping = f"Related_Table.{field_name}" if random.random() > 0.5 else ""
    sample_data = f"Sample_{random.randint(1000, 9999)}"

    data.append([
        company, domain, table_name, f"Description for {table_name}", field_name, field_description,
        data_type, format_pattern, field_length, default_value, nullable_value, source_system,
        relationship_mapping, sample_data
    ])

# Create DataFrame
df = pd.DataFrame(data, columns=[
    "Company", "Domain", "Table Name", "Table Description", "Field Name", "Field Description",
    "Data Type", "Format/Pattern", "Field Length", "Default Value", "Nullable (Yes/No)",
    "Source System", "Relationship Mapping", "Sample Data"
])

# Save to CSV
df.to_csv("data_inventory.csv", index=False)
print("CSV file generated successfully!")