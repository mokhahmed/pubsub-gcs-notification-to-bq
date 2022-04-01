import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("bucket_path")
code_bucket_path = models.Variable.get("code_bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
topic = models.Variable.get("topic")
bqTable = models.Variable.get("bqTable")
filesFormat = models.Variable.get("filesFormat")
schema = models.Variable.get("schema")

default_args = {
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id,
        "zone": gce_zone,
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
        template=code_bucket_path + "/dev/pubsub-gcs-notification-to-bq",
        parameters={
          "topic"=topic,
          "bqTable"=bqTable,
          "filesFormat"=filesFormat,
          "schema"=schema
        },
    )