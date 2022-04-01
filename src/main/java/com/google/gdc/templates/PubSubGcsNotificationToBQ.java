package com.google.gdc.templates;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class PubSubGcsNotificationToBQ {

    private CustomPipelineOptions options;
    private Pipeline pipeline;

    public PubSubGcsNotificationToBQ(){

    }

    public PubSubGcsNotificationToBQ(Pipeline pipeline, CustomPipelineOptions options){
        this.options = options;
        this.pipeline = pipeline;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public CustomPipelineOptions getOptions() {
        return options;
    }

    public void setOptions(CustomPipelineOptions options) {
        this.options = options;
    }


    public  PubsubIO.Read<String> fromPubSub(String topicName){
        return PubsubIO.readStrings().fromTopic(topicName);
    }

    public static  String extractPath(String notification) {
        try {
            Map<String, String> event = (Map<String, String>) new ObjectMapper()
                    .readValue(notification, Map.class).get("event");
            return "gs://" + event.get("bucket") + "/" + event.get("name");
        }catch (JsonProcessingException ex){
            ex.printStackTrace();
            return null;
        }
    }

    public  Map<String, String> parseSchemaString(String schemaString) {
        try {
            Map<String, String> schema = (Map<String, String>) new ObjectMapper().readValue(schemaString, Map.class);
            return schema;
        }catch (JsonProcessingException ex){
            ex.printStackTrace();
            return null;
        }
    }

    public TableSchema getBqSchema(ValueProvider<String> schemaString){
        Map<String, String> schema  =  parseSchemaString(schemaString.toString());
        List<TableFieldSchema> fields = new ArrayList<>();
        schema.forEach( (k, v) -> fields.add(new TableFieldSchema().setName(k).setType(v)));
        return new TableSchema().setFields(fields);
    }

    public MapElements<String, TableRow> toBqRow(){

        return MapElements.into(new TypeDescriptor<TableRow>() {}).via( (String record) -> {
            TableSchema schema = getBqSchema(getOptions().getSchema());
            TableRow row = new TableRow();

            List<TableFieldSchema> fieldsSchema = schema.getFields();
            String[] fields = record.split(",");

            for(int i =0;i<fieldsSchema.size();i++)
                row.set(fieldsSchema.get(i).getName(), fields[i]);

            return row;
        });
    }

    public MapElements<String, String> getFilePath(){
        return MapElements.into(TypeDescriptors.strings()).via( (String n) -> extractPath(n));
    }
    public BigQueryIO.Write<TableRow> writeToBq(String outputTable, TableSchema tableSchema){
        return BigQueryIO
                .writeTableRows()
                .to(outputTable)
                .withSchema(tableSchema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);
    }


    public Pipeline build(){

        String topicName = "projects/" + getOptions().getProject() + "/topics/"+ getOptions().getTopic().toString();
        ValueProvider<String> schema = getOptions().getSchema();
        String outputTable = getOptions().getBqTable().toString();
        TableSchema tableSchema = getBqSchema(schema);

        PCollection<String>  notifications = getPipeline().apply("Read GCS Notifications", fromPubSub(topicName));
        PCollection<String> files = notifications.apply("Extract File Path", getFilePath());
        PCollection<String> contents = files.apply("Read Files", TextIO.readAll());
        //PCollection<TableRow> rows = contents.apply("To BQ Row", toBqRow());
        //rows.apply("Write to BigQuery",writeToBq(outputTable, tableSchema));

        ValueProvider s = ValueProvider.StaticValueProvider.of("test");
         return getPipeline();
    }


}
