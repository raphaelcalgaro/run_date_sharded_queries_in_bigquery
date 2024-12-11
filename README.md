
#Voici le contenu du README en texte brut, sans aucun balisage :

Run Date-Sharded Queries with BigQuery

This Python script automates the process of running queries on date-sharded tables in Google BigQuery. The script allows you to check for the existence of source and destination tables, generate a date range, and copy or process data for specific dates based on preloaded checks.

##Features:

**Authentication**: Uses a JSON key for a service account to authenticate with Google BigQuery.
**Dynamic Date Handling**: Automatically generates a list of dates between a start and end date.
**Table Validation**: Checks for the existence of source and destination tables before executing queries.
**Custom Queries**: Executes queries to process and copy data from source tables to destination tables.

##Prerequisites:

1. Google Cloud Project: Ensure you have a Google Cloud project with BigQuery enabled.
2. Service Account:
- Create a service account with the BigQuery Data Editor role.
- Download the JSON key for the service account.
3. Python Environment:
- Install the required Python libraries: 
```python
pip install google-cloud-bigquery colorama
```


##Setup:

1. Clone the Repository: git clone https://github.com/your-username/your-repository.git cd your-repository
2. Set Up Authentication:
- Replace the placeholder in the script with the path to your JSON key: 
```python
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\path\to\your\service_account.json"
```
3. Define Variables:
- Update the following variables in the script: 
```python
START_DATE_INCLUDED = "YYYY-MM-DD"
END_DATE_INCLUDED = "YYYY-MM-DD"

SOURCE_PROJECT_ID = "your-source-project-id"
SOURCE_DATASET_ID = "your-source-dataset-id"
SOURCE_TABLE_PREFIX = "your-source-table-prefix"

DESTINATION_PROJECT_ID = "your-destination-project-id"
DESTINATION_DATASET_ID = "your-destination-dataset-id"
DESTINATION_TABLE_PREFIX = "your-destination-table-prefix"
```

#How It Works:

1. Authentication:
- The script authenticates with Google BigQuery using a service account JSON key. The service account must have the appropriate permissions (e.g., BigQuery Data Editor).
2. Generate Date Range:
- The script generates a list of dates between the specified START_DATE_INCLUDED and END_DATE_INCLUDED. These dates are used to identify the corresponding sharded source tables.
3. Check Existing Tables:
- The script retrieves the list of source and destination tables:
   - Checks if each source table exists for the given date.
   - Checks if a destination table already exists for the date to avoid overwriting it.
   - If a table doesn't exist, the script skips processing for that date.
4. Run Queries:
- For each valid date:
   - The script constructs a BigQuery SQL query that processes data from the source table.
   - It creates or updates the corresponding destination table using the query results.
   - The query logic can be customized to suit specific requirements (e.g., aggregations, filtering).

#Error Handling:
- The script logs any errors that occur during table validation or query execution to the console. It also ensures that missing source tables and already existing destination tables are skipped gracefully.

# Usage: 
- Run the script from your terminal: python run_datesharded_queries.py

# Customizing the Query:
To modify the query logic, edit the `run_query_for_date` function:
```python
 query = f""" 
 CREATE OR REPLACE TABLE {DESTINATION_DATASET_ID}.{DESTINATION_TABLE_PREFIX}{date} AS 
 SELECT 
   something, 
   somethingElse, 
   COUNT(*) AS total_something 
 FROM 
   {SOURCE_PROJECT_ID}.{SOURCE_DATASET_ID}.{SOURCE_TABLE_PREFIX}{date} 
 GROUP BY 
   something, 
   somethingElse 
 ORDER BY 
   total_something DESC """
```
Example Output: 
```python
No source table found for date 2024-05-15. Skipping. 
The table page_views_20240516 already exists. Skipping. 
Table example_20240517 successfully created.
```python

#License: This project is licensed under the MIT License.