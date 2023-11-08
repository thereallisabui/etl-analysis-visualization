import csv
import mysql.connector
import pandas as pd
import numpy as np  # Import numpy to handle NaN values

# MySQL database connection details
host = 'localhost'
user = 'root'
password = 'MyPassword'
database = 'mrtssales'

# CSV file path and name
csv_file = 'mrtssales.csv'

# Connect to the MySQL database
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
cursor = connection.cursor()

# Create a table in the database
table_name = 'mrts'
create_table_query = '''
    CREATE TABLE IF NOT EXISTS {} (
        kind_of_business VARCHAR(500) NULL,
        year INT NULL,
        date DATE NULL,
        sales_value BIGINT NULL
    )
'''.format(table_name)
cursor.execute(create_table_query)
print("Table created successfully")

# Function to parse integer values, handling special cases and NaN values
def parse_int_value(value):
    if pd.isna(value) or value.strip() == "(S)" or value.strip() == "nan":
        return None
    cleaned_value = value.replace(',', '').strip()
    return int(cleaned_value) if cleaned_value else None

# Read and process the CSV file using pandas
data = pd.read_csv(csv_file)
print("CSV file read successfully")

# Melt the dataframe to convert it into long format
melted_data = pd.melt(data, id_vars=['Kind of Business', 'Year'], var_name='Month', value_name='Sales Value')
print(melted_data)

# Combine 'Month' and 'Year' columns into a new 'date' column with the format YYYY-MM-DD
melted_data['date'] = melted_data['Month'] + ' ' + melted_data['Year'].astype(str)
melted_data['date'] = pd.to_datetime(melted_data['date'], format='%B %Y')
melted_data = melted_data.drop(['Month'], axis=1)

# Iterate through the melted data and insert it into the MySQL table
for index, row in melted_data.iterrows():
    insert_query = '''
        INSERT INTO {} (kind_of_business, year, date, sales_value)
        VALUES (%s, %s, %s, %s)
    '''.format(table_name)

    # Check for NaN values and convert them to None
    values = (
        row['Kind of Business'], row['Year'], row['date'], 
        parse_int_value(row['Sales Value']) if not pd.isna(row['Sales Value']) else None
    )

    cursor.execute(insert_query, values)

print("Data inserted successfully")

# Commit the changes and close the database connection
connection.commit()
connection.close()
print("Database connection closed")
