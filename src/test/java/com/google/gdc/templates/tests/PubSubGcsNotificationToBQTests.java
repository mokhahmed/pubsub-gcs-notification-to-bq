package com.google.gdc.templates.tests;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PubSubGcsNotificationToBQTests {


    public static  TextIO.Read fromFile(String path){
        return TextIO.read().from(path);
    }

    public static  String extractPath(String notification) {
        try {
            Map<String, String> event = (Map<String, String>) new ObjectMapper().readValue(notification, Map.class).get("event");
            return event.get("bucket").toString() + "/" + event.get("name").toString();
        }catch (JsonProcessingException ex){
         ex.printStackTrace();
        }
        return null;
    }

    public static Map<String, String> parseSchemaString(String schemaString) {
        try {
            Map<String, String> schema = (Map<String, String>) new ObjectMapper().readValue(schemaString, Map.class);
            return schema;
        }catch (JsonProcessingException ex){
            ex.printStackTrace();
            return null;
        }
    }

    public static TableSchema getBqSchema(String schemaString){
        Map<String, String> schema  =  parseSchemaString(schemaString);
        List<TableFieldSchema> fields = new ArrayList<>();
        schema.forEach( (k, v) -> fields.add(new TableFieldSchema().setName(k).setType(v)));
        return new TableSchema().setFields(fields);
    }

    public static TableRow toBqRow(String r, String schemaString){
        TableSchema schema  = getBqSchema(schemaString);
        TableRow row = new TableRow();
        List<TableFieldSchema> fieldsSchema = schema.getFields();
        String[] fields =r.split(",");

        for(int i =0;i<fieldsSchema.size();i++)
            row.set(fieldsSchema.get(i).getName(), fields[i]);

        return row;
    }


    public static void main(String[] args) {

        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions testOption = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);

        Pipeline p = Pipeline.create(testOption);

        String path = "src/test/resources/notification/notification.avro";
        String schemaString = "{\"name\":\"string\", \"age\":\"string\", \"country\":\"string\"}";
        PCollection<String> notifications= p.apply(fromFile(path));
        notifications
                .apply(MapElements.into(TypeDescriptors.strings()).via( record -> extractPath(record)))
                .apply(TextIO.readAll())
                .apply(MapElements.into(new TypeDescriptor<TableRow>(){}).via( r -> toBqRow(r, schemaString)))
                .apply(MapElements.into(TypeDescriptors.voids()).via(x -> {System.out.println(x);return null;}));

        p.run();


    }

}
