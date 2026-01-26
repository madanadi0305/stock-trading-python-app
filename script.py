import os
import dotenv
import requests
import openai
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector
import time
load_dotenv()
STOCK_API_KEY = os.getenv("API_KEY")
limit=1000  # Per page limit
MAX_TOTAL_TICKERS = 1000  # Total limit to stop collection
url="https://api.polygon.io/v3/reference/tickers?apiKey="+str(STOCK_API_KEY)+"&limit="+str(limit)

def run_stock_job():
    tickers = []  # Initialize tickers list inside the function
    try:
        response=requests.get(url)
        response.raise_for_status()
        print(response.status_code)
        data=response.json()
        next_url=data['next_url']
        print(data)
        print(data.keys())
        
        # Collect tickers from first response
        for ticker in data['results']:
            if len(tickers) >= MAX_TOTAL_TICKERS:
                print(f"Reached maximum limit of {MAX_TOTAL_TICKERS} tickers. Stopping collection.")
                break
            tickers.append(ticker)
        
        while(next_url is not None and len(tickers) < MAX_TOTAL_TICKERS):
            try:
                # Add delay to respect rate limits (12 seconds for free tier ~5 req/min)
                time.sleep(12)
                print(f"Making request to: {next_url[:50]}...")
                
                response=requests.get(next_url+"&apiKey="+str(STOCK_API_KEY))
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    print(f"Rate limit hit. Waiting {retry_after} seconds before retrying...")
                    time.sleep(retry_after)
                    continue  # Retry the same URL
                
                response.raise_for_status()
                data=response.json()
                next_url=data.get('next_url')  # Use .get() to handle missing key
                print("Next URL:",next_url)
                print(data.keys())
                
                for ticker in data['results']:
                    if len(tickers) >= MAX_TOTAL_TICKERS:
                        print(f"Reached maximum limit of {MAX_TOTAL_TICKERS} tickers. Stopping collection.")
                        break
                    tickers.append(ticker)
                    
                print(f"Collected {len(tickers)} tickers so far...")
                
                # Check if we've reached the limit after adding tickers
                if len(tickers) >= MAX_TOTAL_TICKERS:
                    print(f"Reached maximum limit of {MAX_TOTAL_TICKERS} tickers. Stopping collection.")
                    break
                
            except requests.exceptions.RequestException as e:
                print(f"Error during pagination: {e}")
                print(f"Collected {len(tickers)} tickers before error. Proceeding with available data...")
                break
        
        # Load data directly into Snowflake (skip CSV creation)
        print(f"Total tickers collected: {len(tickers)}")
        
        # Load into Snowflake
        if len(tickers) > 0:
            load_into_snowflake(tickers)
    except requests.exceptions.RequestException as e:
        print(e)

### Load data into existing Snowflake table
def load_into_snowflake(ticker_data):
    try:
        conn=snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
        cursor=conn.cursor()
        
        # Get existing table structure
        cursor.execute("DESCRIBE TABLE stock_ticker")
        existing_columns = [row[0].upper() for row in cursor.fetchall()]
        print(f"Existing table columns: {existing_columns}")
        
        # Clear existing data and insert new data
        cursor.execute("TRUNCATE TABLE stock_ticker")
        print("Cleared existing data from stock_ticker table")
        
        # Insert data row by row matching existing table structure
        for ticker in ticker_data:
            # Build dynamic insert based on existing columns and available data
            columns_to_insert = []
            values_to_insert = []
            
            for column in existing_columns:
                column_lower = column.lower()
                if column_lower in ticker:
                    columns_to_insert.append(column)
                    values_to_insert.append(f"%({column_lower})s")
            
            if columns_to_insert:
                insert_sql = f"""
                INSERT INTO stock_ticker ({', '.join(columns_to_insert)})
                VALUES ({', '.join(values_to_insert)})
                """
                cursor.execute(insert_sql, ticker)
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully loaded {len(ticker_data)} tickers into Snowflake!")
    except Exception as e:
        print(f"Snowflake error: {e}")

if __name__ == "__main__":
    run_stock_job()
