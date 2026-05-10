from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression

import pandas as pd
import joblib
import boto3
import os


S3_BUCKET = "async-ai-inference-bucket"

LOCAL_MODEL_PATH = "/tmp/model.pkl"
LOCAL_TEST_DATA_PATH = "/tmp/test_data.csv"


def train_and_upload_model():
    # 1. Load breast cancer dataset
    data = load_breast_cancer()
    X = data.data
    y = data.target

    # 2. Split train/test data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # 3. Train simple sklearn model
    model = LogisticRegression(max_iter=5000)
    model.fit(X_train, y_train)

    # 4. Save model locally
    joblib.dump(model, LOCAL_MODEL_PATH)

    # 5. Save test data locally
    test_df = pd.DataFrame(X_test, columns=data.feature_names)
    test_df["target"] = y_test
    test_df.to_csv(LOCAL_TEST_DATA_PATH, index=False)

    # 6. Upload model and test data to S3
    s3 = boto3.client("s3")

    s3.upload_file(
        LOCAL_MODEL_PATH,
        S3_BUCKET,
        "models/model.pkl"
    )

    s3.upload_file(
        LOCAL_TEST_DATA_PATH,
        S3_BUCKET,
        "data/test_data.csv"
    )

    print("Model and test data uploaded to S3 successfully.")


with DAG(
    dag_id="training_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    description="Train breast cancer model and upload model/test data to S3",
) as dag:

    train_task = PythonOperator(
        task_id="train_and_upload_model",
        python_callable=train_and_upload_model
    )

    train_task