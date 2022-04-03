import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.utils.dates import days_ago

#bucket_path = models.Variable.get("bucket_path")
#code_bucket_path = models.Variable.get("code_bucket_path")
#project_id = models.Variable.get("project_id")
#gce_zone = models.Variable.get("gce_zone")

bucket_path='gs://lg-workshop-common-tooling'
project_id = 'lg-workshop-common-tooling'
region = 'europe-central2'
topic='prj-x-customer-y-eu-raw-data-files-log-topic'
bqTable='prj_x_customer_y_eu_raw_data.persons'
filesFormat='csv'
schema='{\"name\":\"string\", \"age\":\"string\", \"country\":\"string\"}'

default_args = {
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id,
        "location": region,
        "tempLocation": bucket_path + "/tmp/",
    },
}

with models.DAG(
    "pubsub_gcs_notification_to_bq_dag",
    default_args=default_args,
    schedule_interval='@once',
) as dag:

    start_template_job = DataflowTemplatedJobStartOperator(
        task_id="dataflow_operator_transform_csv_to_bq",
        template="gs://lg-workshop-common-tooling/dataflow_templates/PubSubGcsNotificationToBQ",
        parameters={
          "topic": topic,
          "bqTable": bqTable,
          "filesFormat": filesFormat,
          "schema": schema
        }
    )