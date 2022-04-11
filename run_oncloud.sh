#!/bin/bash

if [ "$#" -lt 3 ]; then
   echo "Usage:   ./run_oncloud.sh project-name bucket-name classname [options] "
   exit
fi

PROJECT=$1
shift
BUCKET=$1
shift
MAIN=com.google.gdc.runners.$1
shift

echo "Launching $MAIN project=$PROJECT bucket=$BUCKET $*"

mvn compile -e exec:java \
 -Dexec.mainClass=$MAIN \
      -Dexec.args="--project=$PROJECT \
      --topic=\
      --bqTable=\
      --inputFilesFormat=\
      --fileDelimiter=\
      --inputSchema=\
      --outputSchema=\
      --region=europe-central2\
      --stagingLocation=gs://$BUCKET/staging/ $* \
      --tempLocation=gs://$BUCKET/staging/ \
      --runner=DataflowRunner"