import requests
import pyodbc
from datetime import datetime
from dotenv import load_dotenv
import os
from prettytable import PrettyTable
import matplotlib.pyplot as plt

# Load environment variables from .env file
load_dotenv()
db_server = os.getenv("DB_SERVER")
db_database = os.getenv("DB_DATABASE")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
ibm_token=os.getenv("IBM_TOKEN")

# Connect to SQL Server database   
conn = pyodbc.connect(
    "DRIVER={SQL Server};"
    f"SERVER={db_server};"
    f"DATABASE={db_database};"
    f"UID={db_username};"
    f"PWD={db_password};"
)

# Function to create the schema for storing weather data
def create_schema(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'your_table')
            BEGIN
            CREATE TABLE your_table (
            [Formatted Date] DATETIME,
            [Summary] NVARCHAR(255),
            [Precip Type] NVARCHAR(255),
            [Temperature (C)] FLOAT,
            [Apparent Temperature (C)] FLOAT,
            [Humidity] FLOAT,
            [Wind Speed (km/h)] FLOAT,
            [Wind Bearing (degrees)] FLOAT,
            [Visibility (km)] FLOAT,
            [Loud Cover] FLOAT,
            [Pressure (millibars)] FLOAT,
            [Daily Summary] NVARCHAR(MAX)
            );
            END; """)
        cursor.close()
        print("Table created successfully.")
    except pyodbc.Error as e:
        print("Error creating schema:", e)

# Function to fetch weather data from API
def fetch_your_table():
    url = "https://api.openweathermap.org/data/2.5/weather?q=New+York&appid=8020ec0765bd31c0f34d5bbaa71deb22"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch weather data. Status code:", response.status_code)
        return None

# Main function
def main():
    if conn:
        conn.autocommit = True
        # Create schema for weather data
        create_schema(conn)
        run()

def run():
    # Define queries
    queries = [
        # Select Top 15 Records
        ("Selecting top 15 records", "SELECT TOP 15 * FROM your_table;"),
        
        # Select Specific Columns for Top 15 Records
        ("Selecting specific columns for top 15 records", "SELECT TOP 15 [Formatted Date], [Temperature (C)] FROM your_table;"),
        
        # Select Top 15 Records for Specific Date Range
        ("Selecting top 15 records for specific date range", "SELECT TOP 15 * FROM your_table WHERE [Formatted Date] BETWEEN '2006-04-01 00:00:00.000' AND '2006-04-01 23:00:00.000';"),
        
        # Select Top 15 Records Filtered by Precipitation Type
        ("Selecting top 15 records filtered by precipitation type", "SELECT TOP 15 * FROM your_table WHERE [Precip Type] = 'rain';"),
        
        # Select Top 15 Records Filtered by Temperature Range
        ("Selecting top 15 records filtered by temperature range", "SELECT TOP 15 * FROM your_table WHERE [Temperature (C)] >= 15 AND [Temperature (C)] <= 20;"),
        
        # Select Top 15 Records Ordered by Temperature (Descending)
        ("Selecting top 15 records ordered by temperature (descending)", "SELECT TOP 15 * FROM your_table ORDER BY [Temperature (C)] DESC;"),
        
        # Calculate Average Temperature for Top 15 Records
        ("Calculating average temperature for top 15 records", "SELECT AVG([Temperature (C)]) AS Average_Temperature FROM (SELECT TOP 15 [Temperature (C)] FROM your_table) AS Top15;"),
        
        # Count Rows for Top 15 Records
        ("Counting rows for top 15 records", "SELECT COUNT(*) AS Row_Count FROM (SELECT TOP 15 * FROM your_table) AS Top15;"),
        
        # Group by Precipitation Type and Calculate Average Humidity for Top 15 Records
        ("Grouping by precipitation type and calculating average humidity for top 15 records", "SELECT [Precip Type], AVG([Humidity]) AS Average_Humidity FROM (SELECT TOP 15 [Precip Type], [Humidity] FROM your_table) AS Top15 GROUP BY [Precip Type];")
    ]

    # Execute queries and print results as tables
    with conn.cursor() as cursor:
        for query_title, query in queries:
            try:
                print(f"Query: {query_title}")
                print(f"SQL: {query}")
                cursor.execute(query)
                rows = cursor.fetchall()
                if rows:
                    print_results(rows, cursor.description)
                    if "Temperature" in query:
                        plot_results(rows, cursor.description, query_title)
                else:
                    print("No records found.")
                print("-" * 50)
            except pyodbc.Error as e:
                print("Error executing query:", e)

# Function to print query results as a table
def print_results(rows, columns):
    table = PrettyTable([column[0] for column in columns])
    for row in rows:
        table.add_row(row)
    print(table)

# Function to plot temperature results
# Function to plot temperature results
def plot_results(rows, columns, title):
    data = list(rows)  # Convert rows to a list
    if len(data) > 0:
        temperatures = [row[1] for row in data if len(row) > 1]  # Assuming temperature is the second column
        dates = [row[0] for row in data if len(row) > 0]  # Assuming dates is the first column
        if temperatures and dates:
            plt.figure(figsize=(10, 6))
            plt.plot(dates, temperatures, marker='o', linestyle='-', color='b')
            plt.title(title)
            plt.xlabel('Date')
            plt.ylabel('Temperature (C)')
            plt.xticks(rotation=45)
            plt.grid(True)
            plt.tight_layout()
            plt.show()
        else:
            print("Insufficient data for plotting.")
    else:
        print("No data fetched from the database.")


if __name__ == "__main__":
    main()
