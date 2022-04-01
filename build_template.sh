#!/bin/bash

mvn compile exec:java \
    -Dexec.mainClass=com.google.gdc.runner.PubSubGcsNotificationToBQRunner \
    -Dexec.args="--runner=DataflowRunner \
                --project=lg-workshop-common-tooling \
                --topic=prj-x-customer-y-eu-raw-data-files-log-topic\
                --bqTable=prj_x_customer_y_eu_raw_data.persons\
                --filesFormat=csv\
                --schema='{\"name\":\"string\", \"age\":\"string\", \"country\":\"string\"}'\
                --region=europe-central2\
                --stagingLocation=gs://lg-workshop-common-tooling/staging \
                --templateLocation=gs://lg-workshop-common-tooling/dataflow_templates/PubSubGcsNotificationToBQ" \
    -P dataflow-runner
