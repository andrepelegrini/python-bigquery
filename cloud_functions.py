import json
from datetime import datetime
import pandas as pd
from flatten_json import flatten
import gcsfs
from google.cloud import storage
import base64


def load_raw(message_raw):
    today = datetime.today()
    now = datetime.now()

    bucket_name = 'bucket-name-raw'
    dt_processamento = today.strftime("%Y-%m-%d")
    table_path = 'folder1/folder2/profile/' + dt_processamento + '/'
    filename = 'profile-' + now.strftime("%Y-%m-%d_%H:%M:%S") + '.txt'

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(table_path + filename)
    blob.upload_from_string(message_raw)


def load_trusted(message):
    # Normalize json and rename columns
    data = flatten(message)
    df = pd.DataFrame(data, index=[0])

    df_result = df.rename(columns={'column1': 'column_1',
                                   'column2': 'column_2',
                                   'column3': 'column_3'})

    # Load data to a trusted folder
    today = datetime.today()

    client = storage.Client()
    bucket = client.get_bucket('bucket-name-trusted')

    dt_processamento = today.strftime("%Y-%m-%d")
    partition_dir = 'folder1/folder2/profile/' + dt_processamento + '/'

    now = datetime.now()
    file_name = 'profile-' + now.strftime("%Y-%m-%d_%H:%M:%S") + '.parquet'

    gcs = gcsfs.GCSFileSystem(project='project-id', token=None)

    df_result.to_parquet('gs://bucket-name-trusted/' + partition_dir + file_name, compression='SNAPPY')


def load_data(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    message_raw = base64.b64decode(json.dumps(event['data']))
    message = json.loads(base64.b64decode(json.dumps(event['data'])))

    return load_raw(message_raw), load_trusted(message)