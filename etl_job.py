import boto3
from io import StringIO, BytesIO
import pandas as pd
from prefect import task, flow
from datetime import datetime, timedelta
import pytz
import psycopg2
import os
from sqlalchemy import create_engine

thailand_timezone = pytz.timezone('Asia/Bangkok')

account_name = "nay-krit"
postgres_info = {
    'username': os.environ['postgres_username'],
    'password': os.environ['postgres_password'],
    'host': os.environ['postgres_host'],
    'port': os.environ['postgres_port'],
}

# Get list of date from start date to current date
def get_date_range(start_date_str):

    # Get the Thailand timezone
    
    # Convert the start date string to a datetime object in Thailand timezone
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    start_date = thailand_timezone.localize(start_date)  # Make start_date timezone-aware
    
    # Get the current date and time in Thailand's timezone
    current_time_thailand = datetime.now(thailand_timezone)
    
    # Initialize lists to storepaths
    path_list = []
    
    # Generate dates from start_date to current_date
    while start_date <= current_time_thailand:
        # Generate the path based on the date format specified
        path = start_date.strftime('common/data/partitioned/%Y/%m/%d/transaction.csv')
        path_list.append(path)    
        start_date += timedelta(days=1)

    return path_list

# Extract data from source
s3_client = boto3.client('s3')
bucket_name = 'fullstackdata2023'

@task(retries=3)
def extract(bucket_name):
    '''
    Extract data from S3 bucket
    '''
    start_date_string = '2023-11-30'  
    path_list = get_date_range(start_date_string)

    # Initialize an empty DataFrame to store concatenated data
    full_df = pd.DataFrame()
    
    for source_path in path_list:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=source_path)
        # Read the CSV file content
        csv_string = StringIO(response['Body'].read().decode('utf-8'))
        # Use Pandas to read the CSV file
        df = pd.read_csv(csv_string)
        print(f"*** Extract df from {source_path} with {df.shape[0]} rows ***")
        # Concatenate the df
        full_df = pd.concat([full_df, df], ignore_index=True)

    return full_df  

@task
def transform(df):
    '''
    Transform data
    '''
    # Recency
    df['date'] = pd.to_datetime(df['date'])
    recency_df = df.groupby('customer_id')['date'].max().reset_index()
    recency_df.columns = ['customer_id', 'recency']

    # Frequency
    frequency_df = df.groupby('customer_id')['order_id'].nunique().reset_index()
    frequency_df.columns = ['customer_id', 'frequency']

    # Monetary
    monetary_df = df.groupby('customer_id')['total_revenue'].sum().reset_index()  
    monetary_df.columns = ['customer_id', 'monetary_value']
    monetary_df['monetary_value'] = monetary_df['monetary_value'].apply(lambda x: round(x, 2))

    # Merge all calculated metrics into a single DataFrame
    rfm = recency_df.merge(frequency_df, on='customer_id').merge(monetary_df, on='customer_id')

    # Merge with customer details
    customer_details = df[['customer_id', 'customer_name', 'customer_province']].drop_duplicates()
    customer = customer_details.merge(rfm, on='customer_id')
    customer['snap_date'] = datetime.now(thailand_timezone).strftime('%Y-%m-%d')
    customer['recency'] = customer['recency'].dt.strftime('%Y-%m-%d')
    customer = customer.sort_values(by='customer_id', ascending=True).reset_index(drop=True)

    print(f"*** Transform df to customer with {customer.shape[0]} rows ***")

    return customer

@task 
def load_postgres(customer):
    '''
    Load transformed result to Postgres
    '''
    database = postgres_info['username'] # each user has their own database in their username
    table_name = 'customer'
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)
    print(f"Writing to database {postgres_info['username']}.{table_name} with {customer.shape[0]} records")
    customer.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Write successfully!")

@flow(log_prints=True)
def pipeline():
    '''
    Flow for ETL pipeline
    '''
    df = extract(bucket_name)
    print('Extract data:')
    print(df)

    customer = transform(df)
    print('Transform data:')
    print(customer)

    load_postgres(customer)

if __name__ == '__main__':
    # pipeline()
    pipeline.serve(name='mini_project_DE')





