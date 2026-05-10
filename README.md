# Asynchronous AI Inference System

## Overview

This project builds an asynchronous machine learning inference system using Airflow, AWS S3, AWS SQS, Docker, and Kubernetes.

The system trains a machine learning model using the breast cancer dataset, stores the trained model in S3, sends inference jobs to SQS, and uses Kubernetes consumers to process messages asynchronously and write predictions back to S3.

## System Architecture

### Training Flow

1. Airflow loads the breast cancer dataset.
2. Airflow splits the dataset into train and test sets.
3. A simple scikit-learn model is trained.
4. The trained model is serialized using joblib.
5. The trained model is uploaded to S3 as `models/model.pkl`.
6. The test dataset is uploaded to S3 as `data/test_data.csv`.

### Inference Flow

1. Airflow reads the test dataset from S3.
2. Airflow sends one message per record to SQS.
3. Kubernetes consumers read messages from the SQS queue.
4. Consumers load the trained model from S3 on startup.
5. Consumers perform inference.
6. Consumers write each prediction back to S3.
7. Consumers delete SQS messages only after successful processing.

## Project Structure

```text
async-ai-inference-system/
├── dags/
│   ├── training_dag.py
│   └── queue_population_dag.py
├── consumer/
│   ├── app.py
│   └── requirements.txt
├── k8s/
│   └── consumer-deployment.yaml
├── Dockerfile
└── README.md
``` 

## AWS Resources

1. S3 Bucket
async-ai-inference-bucket

2. S3 stores:
models/model.pkl
data/test_data.csv
predictions/sample_x.json

3. SQS Queue
async-ai-inference-queue
Example SQS message:

{
  "record_id": "sample_001",
  "features": [...]
}

## Airflow Instructions

!. Activate the virtual environment:
source venv/bin/activate

2. Set Airflow home:
export AIRFLOW_HOME=~/environment/async-ai-inference-system/airflow_home

3. Copy DAGs into Airflow:
mkdir -p $AIRFLOW_HOME/dags
cp dags/training_dag.py $AIRFLOW_HOME/dags/
cp dags/queue_population_dag.py $AIRFLOW_HOME/dags/

4. Start the scheduler:
airflow scheduler

5. In another terminal, start the webserver:
airflow webserver --port 8080

6. Run the DAGs in this order:
training_dag
queue_population_dag


## Training DAG.  Image attached as Airflow-dags.png
1. The training_dag.py file:

loads the breast cancer dataset
splits the data into train and test sets
trains a Logistic Regression model
saves the model using joblib
uploads models/model.pkl to S3
uploads data/test_data.csv to S3

Expected S3 outputs: Image attached as Model-pkl.png
models/model.pkl
data/test_data.csv

2. Queue Population DAG   Image attached as SQS-messages and SQS.png
The queue_population_dag.py file:
reads data/test_data.csv from S3
creates one message per test record
sends each message to SQS

## Consumer Application
The consumer application is in:
consumer/app.py

## The consumer:
polls SQS for messages
downloads models/model.pkl from S3 on startup
performs inference
writes each prediction to S3
deletes SQS messages only after successful processing

Each prediction is saved as a unique file: Image attached as s3-prediction.png
predictions/sample_001.json

## Docker Instructions
1. Build the Docker image:
docker build -t inference-consumer:latest .

2. Test the container locally:
docker run \
  -v ~/.aws:/root/.aws \
  -e PYTHONUNBUFFERED=1 \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e S3_BUCKET=async-ai-inference-bucket \
  -e SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/330117118623/async-ai-inference-queue" \
  inference-consumer:latest
  
3. Tag the image:
docker tag inference-consumer:latest kimmypeters/inference-consumer:latest

4. Push the image to Docker Hub:
docker push kimmypeters/inference-consumer:latest

## Kubernetes Instructions
*** The consumer-deployment.yaml file is the Kubernetes Deployment yaml***

1. Create a Kubernetes secret for AWS credentials:
kubectl create secret generic aws-credentials \
  --from-file=credentials=$HOME/.aws/credentials

2. Deploy the consumer:
kubectl apply -f k8s/consumer-deployment.yaml

3. Check the deployment:
kubectl get deployments

4. Check pods:
kubectl get pods

5. View logs:
kubectl logs -f POD_NAME

## Scaling the Consumer

1. The Kubernetes deployment starts with at least one replica.
kubectl scale deployment inference-consumer --replicas=1

2. Scale the consumer to 3 replicas: Image attached as kubernetes-scaled.png
kubectl scale deployment inference-consumer --replicas=3

3. Check that 3 pods are running:
kubectl get pods

4.Expected S3 prediction files: Image attached as Prediction-output.png 
predictions/sample_0.json
predictions/sample_1.json
predictions/sample_2.json

This demonstrates that the asynchronous inference system can scale horizontally.

## Expected Final Output
After running the full pipeline:
training_dag uploads the trained model and test data to S3.
queue_population_dag sends test records to SQS.
Kubernetes consumers process the SQS messages.
Prediction files are written to S3.
