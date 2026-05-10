from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import boto3
import pandas as pd
import json

S3_BUCKET = "async-ai-inference-bucket"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/330117118623/async-ai-inference-queue"

LOCAL_TEST_DATA_PATH = "/tmp/test_data.csv"


def send_messages_to_sqs():

    # Download test data from S3
    s3 = boto3.client("s3")

    s3.download_file(
        S3_BUCKET,
        "data/test_data.csv",
        LOCAL_TEST_DATA_PATH
    )

    # Read CSV
    df = pd.read_csv(LOCAL_TEST_DATA_PATH)

    # Remove target column
    features_df = df.drop(columns=["target"])

    # Create SQS client
    sqs = boto3.client("sqs")

    # Send one message per record
    for index, row in features_df.iterrows():

        message = {
            "record_id": f"sample_{index}",
            "features": row.tolist()
        }

        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message)
        )

    print("Messages sent to SQS successfully.")


with DAG(
    dag_id="queue_population_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    description="Send test records to SQS",
) as dag:

    send_task = PythonOperator(
        task_id="send_messages_to_sqs",
        python_callable=send_messages_to_sqs
    )

    send_task