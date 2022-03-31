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

    public PubSubGcsNotificationToBQ(Pipeline pipeline, CustomPipelineOptions options){
        this.options = options;
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

    public TableSchema getBqSchema(String schemaString){
        Map<String, String> schema  =  parseSchemaString(schemaString);
        List<TableFieldSchema> fields = new ArrayList<>();
        schema.forEach( (k, v) -> fields.add(new TableFieldSchema().setName(k).setType(v)));
        return new TableSchema().setFields(fields);
    }

    public TableRow toBqRow(String r, String schemaString){
        TableSchema schema = getBqSchema(schemaString);
        TableRow row = new TableRow();
        List<TableFieldSchema> fieldsSchema = schema.getFields();
        String[] fields =r.split(",");

        for(int i =0;i<fieldsSchema.size();i++)
            row.set(fieldsSchema.get(i).getName(), fields[i]);

        return row;
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

        String topicName = "projects/" + getOptions().getProject() + "/topics/"+ getOptions().getTopic();
        String schema = getOptions().getSchema();
        String outputTable = getOptions().getBqTable();
        TableSchema tableSchema = getBqSchema(schema);
        PCollection<String>  notifications = getPipeline().apply("Read GCS Notifications", fromPubSub(topicName));

        PCollection<String> files = notifications.apply("Extract File Path",
                MapElements.into(TypeDescriptors.strings()).via( n -> extractPath(n)));

        PCollection<String> content = files.apply("Read Files", TextIO.readAll());

        PCollection<TableRow> rows = files.apply("To BQ Row",
                MapElements.into(new TypeDescriptor<TableRow>() {}).via( r -> toBqRow(r, schema)));

        rows.apply("Write to BigQuery",writeToBq(outputTable, tableSchema));

         return getPipeline();
    }


}