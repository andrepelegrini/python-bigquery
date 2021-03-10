import csv
import os
import numpy as np
from google.cloud import bigquery

# Connecting to BigQuery - table_1
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'key_production.json'
client = bigquery.Client()

query_1 = """SELECT name FROM dataset.table_1"""
job_1 = client.query(query_1)
df_1 = job_1.to_dataframe()

# Connecting to BigQuery - table_2
query_2 = """SELECT name FROM dataset.table_2"""
job_2 = client.query(query_2)
df_2 = job_2.to_dataframe()

# Comparing both tables
# table_1
names_1 = df_1.name.tolist()

# table_2
names_2 = df_2.name.tolist()

lista = list(np.setdiff1d(names_1, names_2))

# Updating local file
for x in lista:
    with open('C:\\Users\\name.lastname\\Desktop\\Folder\\comparing_tables.csv', 'a', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow([x])

        f.close()