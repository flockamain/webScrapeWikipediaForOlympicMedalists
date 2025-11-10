from bs4 import BeautifulSoup
import requests
import pandas as pd
from numbers import Number
from sqlalchemy import create_engine, text

# Scrape Wikipedia data
url = 'https://en.wikipedia.org/wiki/List_of_multiple_Olympic_medalists'
headers = {'User-Agent': 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org)'}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

## getting the table headers
table = soup.find_all('table')[0]
titles = table.find_all('th')
table_titles = [title.text.strip() for title in titles]
df = pd.DataFrame(columns=table_titles)

## getting the table rows
rows = table.find_all('tr')[1:]  # Skip the header row
previous_values = []
for row in rows:
    cols = row.find_all('td')
    cols = [col.text.strip() for col in cols]
    
    # Handle missing columns
    if len(cols) == 9:
        cols.insert(0, previous_values[0])  # Adding rank if missing
        cols.append(previous_values[10])  # Adding total if missing
    elif len(cols) == 10:
        if isinstance(previous_values[0], Number) or previous_values[0].isdigit():
            cols.append(previous_values[10])  # Adding total if missing
        else:
            cols.insert(0, previous_values[0])  # Adding rank if missing
    
    if len(cols) == len(table_titles):  # Ensure the row has the correct number of columns
        length = len(df)
        df.loc[length] = cols
        previous_values = cols

print(df)
print(f"\n[{len(df)} rows x {len(df.columns)} columns]")

# Database configuration
db_username = 'xxx'
db_password = 'xxx'
db_host = 'xxx'
db_port = '5432' #5432 for postgres
db_name = 'xxx'

# Step 1: Connect to the default 'postgres' database to create db1 if needed
print("\nChecking if database exists...")
admin_connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/postgres'

try:
    admin_engine = create_engine(admin_connection_string, isolation_level="AUTOCOMMIT")
    
    with admin_engine.connect() as conn:
        # Check if database exists
        result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :dbname"), {"dbname": db_name})
        exists = result.fetchone()
        
        if not exists:
            print(f"Database '{db_name}' does not exist. Creating it...")
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
            print(f"Database '{db_name}' created successfully!")
        else:
            print(f"Database '{db_name}' already exists.")
    
    admin_engine.dispose()
    
except Exception as e:
    print(f"Error checking/creating database: {e}")
    raise

# Step 2: Connect to db1 and upload the data
print(f"\nConnecting to database '{db_name}'...")
connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(connection_string)

try:
    table_name = 'olympic_medalists'
    print(f"Uploading data to table '{table_name}'...")
    
    df.to_sql(
        table_name, 
        engine, 
        if_exists='replace',  # Options: 'fail', 'replace', 'append'
        index=False
    )
    
    print(f"\n✓ Data uploaded successfully to RDS!")
    print(f"  Database: {db_name}")
    print(f"  Table: {table_name}")
    print(f"  Rows inserted: {len(df)}")
    
except Exception as e:
    print(f"Error uploading data: {e}")
    raise
finally:
    engine.dispose()
