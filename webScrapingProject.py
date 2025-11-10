from bs4 import BeautifulSoup
import requests
import pandas as pd
from numbers import Number
import boto3
from io import StringIO

url = 'https://en.wikipedia.org/wiki/List_of_multiple_Olympic_medalists'
headers = {'User-Agent': 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org)'}


response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

## getting the table headers
table = soup.find_all('table')[0]
titles = table.find_all('th')
table_titles = [title.text.strip() for title in titles]
df = pd.DataFrame(columns=table_titles)
print(df)

## getting the table rows
rows = table.find_all('tr')[1:]  # Skip the header row
previous_values = []
for row in rows:
    cols = row.find_all('td')
    cols = [col.text.strip() for col in cols]
    # If the row has missing columns, there would only be a max of 2 missing
    # because wikipedia combines cells for athletes with the same rank or total medals
    # check if one is missing. If so, the check if the first value is a number
    # if it is, then the rank is present, so the total must be missing
    # if not, then the total is present, so the rank must be missing
    # if both are missing, add both from previous row
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

# Upload to S3
s3_client = boto3.client('s3')
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

bucket_name = 's3bucketforwebscraper'  # Replace with your bucket name
file_name = 'olympic_medalists.csv'

s3_client.put_object(
    Bucket=bucket_name,
    Key=file_name,
    Body=csv_buffer.getvalue()
)

print(f"File uploaded successfully to s3://{bucket_name}/{file_name}")
