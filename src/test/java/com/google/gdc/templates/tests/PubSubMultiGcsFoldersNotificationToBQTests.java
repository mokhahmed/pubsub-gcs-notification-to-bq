package com.google.gdc.templates.tests;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gdc.IO.IOBuilders;
import com.google.gdc.utils.SchemaParser;
import com.google.gdc.utils.Utilities;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.json.JSONObject;

import java.util.Optional;

public class PubSubMultiGcsFoldersNotificationToBQTests {

    public  static MapElements<String, TableRow> stringToBqRow(String schema, String delimiter) {

        return MapElements.into(new TypeDescriptor<TableRow>() {}).via((String record) -> {
            TableSchema tableSchema = null;
            try {
                tableSchema = SchemaParser.convertJsonToTableSchema(schema);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return Utilities.convertStringToBqRow(record, tableSchema, delimiter);
        });
    }



    private static void processSource(PCollection<String> files, JSONObject sourceConfig) throws Exception {

        String inputSchemaPath=sourceConfig.getString("inputSchemaPath");
        String outputTable=sourceConfig.getString("outputBqTableName");
        String outputSchemaPath=sourceConfig.getString("outputSchemaPath");
        String fileDelimiter=sourceConfig.getString("delimiter");
        String sourceName =sourceConfig.getString("name");
        String matcher =sourceConfig.getString("patternMatcher");

        PCollection<String> contents = files
                .apply("Filter Only Source Files",
                        Filter.by((SerializableFunction<String, Boolean>) input -> input.contains(matcher)))
                .apply("lookup Files",
                        FileIO.matchAll())
                .apply("Filter Matches",
                        FileIO.readMatches())
                .apply("Read Files",
                        TextIO.readFiles());

        PCollection<TableRow> rows = contents
                .apply("To BQ Row",
                        stringToBqRow(inputSchemaPath, fileDelimiter));

        rows.apply(MapElements.into(TypeDescriptors.voids()).via(x -> {
                            System.out.println(x);
                            return null;
        }));
    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions testOption = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);

        Pipeline p = Pipeline.create(testOption);

        String inputPath = "src/test/resources/input_data/notification/notification.avro";
        String inputSchemaPath = "src/test/resources/input_data/notification/job_schema.json";

        try {

            JSONObject jobConfig = SchemaParser.parseSchemaFile(inputSchemaPath);

            PCollection<String> notifications = p.apply(IOBuilders.fromFile(inputPath));

            PCollection<String> allNotification = notifications
                    .apply(MapElements.into(TypeDescriptors.strings())
                            .via(record -> Utilities.extractPath(record,"")));

            for(String key: jobConfig.keySet())
                processSource(allNotification, jobConfig.getJSONObject(key));

            p.run();

        }  catch (Exception ex){
            ex.printStackTrace();
        }

    }

}
