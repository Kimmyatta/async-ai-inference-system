import json
import os
import time
from datetime import datetime, timezone

import boto3
import joblib
import numpy as np


S3_BUCKET = os.getenv("S3_BUCKET", "async-ai-inference-bucket")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/330117118623/async-ai-inference-queue")

MODEL_S3_KEY = "models/model.pkl"
LOCAL_MODEL_PATH = "/tmp/model.pkl"

s3 = boto3.client("s3")
sqs = boto3.client("sqs")


def load_model():
    print("Downloading model from S3...")
    s3.download_file(S3_BUCKET, MODEL_S3_KEY, LOCAL_MODEL_PATH)
    model = joblib.load(LOCAL_MODEL_PATH)
    print("Model loaded successfully.")
    return model


def write_prediction_to_s3(record_id, prediction):
    result = {
        "record_id": record_id,
        "prediction": int(prediction),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    s3_key = f"predictions/{record_id}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(result),
        ContentType="application/json"
    )

    print(f"Prediction written to s3://{S3_BUCKET}/{s3_key}")


def process_message(message, model):
    body = json.loads(message["Body"])

    record_id = body["record_id"]
    features = body["features"]

    X = np.array(features).reshape(1, -1)
    prediction = model.predict(X)[0]

    write_prediction_to_s3(record_id, prediction)


def main():
    model = load_model()

    print("Consumer started. Polling SQS...")

    while True:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        messages = response.get("Messages", [])

        if not messages:
            print("No messages found. Waiting...")
            time.sleep(5)
            continue

        for message in messages:
            try:
                process_message(message, model)

                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=message["ReceiptHandle"]
                )

                print("Message processed and deleted from SQS.")

            except Exception as e:
                print(f"Error processing message: {e}")
                print("Message was NOT deleted, so it can be retried.")


if __name__ == "__main__":
    main()