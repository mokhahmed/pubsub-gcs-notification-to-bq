package com.google.gdc.templates.tests;

import com.google.api.services.bigquery.model.JsonObject;
import com.google.api.services.bigquery.model.TableFieldSchema;
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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.json.JSONObject;

import java.util.List;

public class PubSubGcsNotificationToBQTests {




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

    public static void main(String[] args) {

        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions testOption = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);

        Pipeline p = Pipeline.create(testOption);

        String inputPath = "src/test/resources/notification/notification.avro";
        String inputSchemaPath = "src/test/resources/notification/input_schema.json";
        //String schemaString = "{\"name\":\"string\", \"age\":\"string\", \"country\":\"string\"}";
        String delimiter = "\\|";
        PCollection<String> notifications= p.apply(IOBuilders.fromFile(inputPath));

        notifications
                .apply(MapElements.into(TypeDescriptors.strings()).via(record -> Utilities.extractPath(record, "")))
                .apply(FileIO.matchAll())
                .apply(FileIO.readMatches())
                .apply(TextIO.readFiles())
                .apply(stringToBqRow(inputSchemaPath, delimiter))
                .apply(MapElements.into(TypeDescriptors.voids()).via(x -> {System.out.println(x);return null;}));

        p.run();


    }

}
