from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import os

def modulo_limite(ri):
    bucket_name = 'bucket-name'
    schema_path_gcs = 'schemas/schema_folder/'
    schema_name_gcs = 'schema_file.json'

    # Download schema and store it at a local variable
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob_schema = bucket.blob(schema_path_gcs + schema_name_gcs)
    blob_schema.download_to_filename(schema_name_gcs)
    schema_fields = client.schema_from_json(schema_name_gcs)
    os.remove(schema_name_gcs)

    # Job Configurations
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.schema = schema_fields

    for df in ri.to_dataframe_iterable():
        df['column'] = 8.55

        df.to_gbq(destination_table='dataset.table',
                  project_id='project-id-name',
                  if_exists='replace')


# Credentials GCP and BigQuery
key_path = 'cert/key.json'

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform",
            "https://spreadsheets.google.com/feeds",
            'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/urlshortener",
            "https://www.googleapis.com/auth/sqlservice.admin",
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/compute",
            "https://www.googleapis.com/auth/devstorage.full_control",
            "https://www.googleapis.com/auth/logging.admin",
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/monitoring",
            "https://www.googleapis.com/auth/servicecontrol",
            "https://www.googleapis.com/auth/service.management.readonly",
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/datastore",
            "https://www.googleapis.com/auth/taskqueue",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/trace.append",
            "https://www.googleapis.com/auth/plus.login",
            "https://www.googleapis.com/auth/plus.me",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile"],
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)

# Query - BigQuery
query = """SELECT * FROM dataset.table"""

query_job = client.query(query)
ri = query_job.result()

modulo_limite(ri)