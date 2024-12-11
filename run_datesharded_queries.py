# -*- coding: utf-8 -*-
"""
Created on Wed Dec 11 15:32:43 2024

@author: Raphael
"""

from colorama import Fore, Style
from datetime import datetime, timedelta
from google.cloud import bigquery
import os

# Step 1: Configure authentication with the JSON key
# The service account associated with the key must have BigQuery Data Editor access
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\{userName}\{fileName}\{json-name}.json"

# Step 2: Initialize the BigQuery client
client = bigquery.Client()

# Step 3: Define variables in the YYYY-MM-DD format, which will automatically be transformed into YYYYMMDD
# Dates
START_DATE_INCLUDED = "2024-05-14"
END_DATE_INCLUDED = "2024-12-10"

# Source
SOURCE_PROJECT_ID = "{{source_project_id}}"
SOURCE_DATASET_ID = "{{source_dataset_id}}"
SOURCE_TABLE_PREFIX = "{{source_table_prefix for GA4:events_}}"

# Destination
DESTINATION_PROJECT_ID = "{{destination_project_id}}"
DESTINATION_DATASET_ID = "{{destination_dataset_id}}"
DESTINATION_TABLE_PREFIX = "{{destination_table_prefix}}"


# Step 4: Functions to execute queries with dynamic checks
# Step 4.1: Function to check tables in a single query
def get_existing_tables(project_id, dataset_id, prefix):
    """
    Retrieve all existing tables in a given dataset that start with a specific prefix.
    :param project_id: Project ID.
    :param dataset_id: Dataset ID.
    :param prefix: Prefix of the tables to search for.
    :return: A set containing the names of existing tables.
    """
    query = f"""
    SELECT table_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE '{prefix}%'
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        return {row.table_name for row in results}
    except Exception as e:
        print(Fore.MAGENTA + f"Error retrieving tables in {project_id}.{dataset_id}: {e}" + Style.RESET_ALL)
        return set()

# Step 4.2: Function to run the query
def run_query_for_date(date, existing_source_tables, existing_destination_tables):
    """
    Run a query to copy data for a specific date,
    if the source and destination table checks pass.

    :param date: The date in 'YYYYMMDD' format.
    :param existing_source_tables: Set of existing source table names.
    :param existing_destination_tables: Set of existing destination table names.
    """
    source_table_name = f"{SOURCE_TABLE_PREFIX}{date}"
    destination_table_name = f"{DESTINATION_TABLE_PREFIX}{date}"
    
    # Check if the source table exists
    if source_table_name not in existing_source_tables:
        print(f"No source table found for date {date}. Skipping.")
        return
    
    # Check if the destination table already exists
    if destination_table_name in existing_destination_tables:
        print(f"The table {destination_table_name} already exists. Skipping.")
        return
    
    # Build and execute the query
    query = f"""
    CREATE OR REPLACE TABLE `{DESTINATION_DATASET_ID}.{DESTINATION_TABLE_PREFIX}{date}` AS
    SELECT
      something,
      somethingElse,
      COUNT(*) AS total_something
    FROM
      `{SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.{SOURCE_TABLE_PREFIX}{date}`
    GROUP BY
      something,
      somethingElse
    ORDER BY
      total_something DESC
    """
    
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for the query to complete
        print(Fore.CYAN + f"Table {destination_table_name} successfully created." + Style.RESET_ALL)
        # Add the created table to the set of existing tables
        existing_destination_tables.add(destination_table_name)
    except Exception as e:
        print(Fore.YELLOW + f"Error executing query for date {date}: {e}" + Style.RESET_ALL)


# Step 5: Generate the list of dates
def generate_date_range(start_date, end_date):
    """
    Generate a list of dates between a start date and an end date (inclusive).

    :param start_date: Start date in 'YYYY-MM-DD' format.
    :param end_date: End date in 'YYYY-MM-DD' format.
    :return: List of dates in 'YYYYMMDD' format.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    date_list = []
    while start <= end:
        date_list.append(start.strftime("%Y%m%d"))
        start += timedelta(days=1)
    
    return date_list

# Step 6: Optimize checks
# Retrieve existing tables for the source and destination
existing_source_tables = get_existing_tables(SOURCE_PROJECT_ID, SOURCE_DATASET_ID, SOURCE_TABLE_PREFIX)
existing_destination_tables = get_existing_tables(DESTINATION_PROJECT_ID, DESTINATION_DATASET_ID, DESTINATION_TABLE_PREFIX)

# Generate the list of dates
dates = generate_date_range(START_DATE_INCLUDED, END_DATE_INCLUDED)

# Step 7: Execute queries based on preloaded checks
for date in dates:
    source_table_name = f"{SOURCE_TABLE_PREFIX}{date}"
    destination_table_name = f"{DESTINATION_TABLE_PREFIX}{date}"
    
    # Check if the source table exists
    if source_table_name not in existing_source_tables:
        print(f"No source table found for date {date}. Skipping.")
        continue
    
    # Check if the destination table already exists
    if destination_table_name in existing_destination_tables:
        print(f"The table {destination_table_name} already exists. Skipping.")
        continue
    
    # Execute the query
    run_query_for_date(date, existing_source_tables, existing_destination_tables)
