import pandas as pd
import numpy as np
from faker import Faker
import random
import datetime
import uuid
import os

# Initialize Faker for generating random data
fake = Faker()
Faker.seed(42)  # For reproducibility
np.random.seed(42)

def generate_securities_transaction_data(num_records=1000):
    """
    Generate random securities transaction data
    
    Args:
        num_records (int): Number of transaction records to generate
        
    Returns:
        DataFrame: Generated transaction data
    """
    print(f"Generating {num_records} securities transaction records...")
    
    # Define possible transaction types
    transaction_types = ['BUY', 'SELL', 'DIVIDEND', 'RIGHTS_ISSUE', 'BONUS', 'TRANSFER']
    payment_methods = ['BANK_TRANSFER', 'DIRECT_DEBIT', 'CASH', 'CHEQUE', 'MOBILE_PAYMENT']
    status_options = ['COMPLETED', 'PENDING', 'CANCELLED', 'FAILED', 'REVERSED']
    asset_classes = ['EQUITY', 'BOND', 'ETF', 'MUTUAL_FUND', 'TREASURY_BILL']
    
    # List of Nigerian stock tickers
    tickers = [
        'DANGCEM', 'NESTLE', 'MTNN', 'GTCO', 'ZENITHBANK', 
        'AIRTEL', 'BUACEMENT', 'FBNH', 'SEPLAT', 'ACCESSCORP',
        'UBA', 'TRANSCORP', 'OANDO', 'FLOURMILL', 'DANGSUGAR',
        'GUINNESS', 'NB', 'PRESCO', 'CADBURY', 'UACN'
    ]
    
    # Generate broker names
    broker_names = [
        f"{fake.company()} Securities",
        f"{fake.company()} Capital",
        f"{fake.company()} Investments",
        f"{fake.last_name()} & {fake.last_name()} Stockbrokers",
        f"{fake.company()} Asset Management"
    ]
    
    # Generate customer IDs (use BVN-like format)
    customer_ids = [str(np.random.randint(1000000000, 9999999999)) for _ in range(num_records//10)]
    
    # Generate random dates within the last 2 years
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=730)  # 2 years ago
    
    # Create empty lists for each column
    data = {
        'transaction_id': [],
        'transaction_date': [],
        'settlement_date': [],
        'customer_id': [],
        'ticker': [],
        'transaction_type': [],
        'quantity': [],
        'price': [],
        'total_amount': [],
        'broker': [],
        'asset_class': [],
        'payment_method': [],
        'status': [],
        'fees': [],
        'tax': []
    }
    
    # Generate data for each record
    for i in range(num_records):
        # Generate transaction date
        txn_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        # Settlement date is typically T+2 or T+3 for completed transactions
        settlement_days = random.randint(2, 5)
        settlement_date = txn_date + datetime.timedelta(days=settlement_days)
        
        # Select other random values
        ticker = random.choice(tickers)
        txn_type = random.choice(transaction_types)
        customer_id = random.choice(customer_ids)
        status = random.choice(status_options)
        
        # Generate appropriate quantity based on transaction type
        if txn_type in ['BUY', 'SELL']:
            quantity = random.randint(10, 10000)
        elif txn_type == 'DIVIDEND':
            quantity = random.randint(100, 50000)
        else:
            quantity = random.randint(50, 5000)
            
        # Generate appropriate price based on ticker
        base_price = random.uniform(5.0, 1000.0)
        if ticker in ['DANGCEM', 'NESTLE', 'MTNN', 'AIRTEL', 'BUACEMENT']:
            # Higher priced stocks
            price = round(base_price * 2 + random.uniform(0, 50), 2)
        else:
            # Lower priced stocks
            price = round(base_price / 2 + random.uniform(0, 10), 2)
            
        # Calculate total amount
        if txn_type in ['BUY', 'SELL']:
            total = quantity * price
        elif txn_type == 'DIVIDEND':
            # Dividend is usually much smaller
            total = quantity * (price * 0.01)
        else:
            total = quantity * price * 0.05
            
        # Calculate fees (0.5% - 2%)
        fee_rate = random.uniform(0.005, 0.02)
        fees = round(total * fee_rate, 2)
        
        # Calculate tax (7.5% VAT in Nigeria)
        tax = round(fees * 0.075, 2)
        
        # Add data to lists
        data['transaction_id'].append(str(uuid.uuid4()))
        data['transaction_date'].append(txn_date)
        data['settlement_date'].append(settlement_date if status != 'CANCELLED' else None)
        data['customer_id'].append(customer_id)
        data['ticker'].append(ticker)
        data['transaction_type'].append(txn_type)
        data['quantity'].append(quantity)
        data['price'].append(price)
        data['total_amount'].append(round(total, 2))
        data['broker'].append(random.choice(broker_names))
        data['asset_class'].append(random.choice(asset_classes))
        data['payment_method'].append(random.choice(payment_methods))
        data['status'].append(status)
        data['fees'].append(fees)
        data['tax'].append(tax)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Format dates properly
    df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['settlement_date'] = pd.to_datetime(df['settlement_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df

def main():
    # Create output directory if it doesn't exist
    output_dir = '/home/diyiola/learning/projects/data-profiling/data/dummy/dummy-data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Define output file path
    output_file = os.path.join(output_dir, 'securities_transactions.csv')
    
    # Generate transaction data
    transactions = generate_securities_transaction_data(num_records=2500)
    
    # Save data to CSV file
    transactions.to_csv(output_file, index=False)
    print(f"Securities transaction data saved to: {output_file}")
    
    # Display sample data
    print("\nSample data (first 5 rows):")
    print(transactions.head())
    
    # Display summary statistics
    print("\nSummary statistics:")
    print(f"Total transactions: {len(transactions)}")
    print(f"Unique customers: {transactions['customer_id'].nunique()}")
    print(f"Transaction types: {transactions['transaction_type'].value_counts().to_dict()}")
    print(f"Transaction status: {transactions['status'].value_counts().to_dict()}")
    
    return transactions

if __name__ == "__main__":
    main()