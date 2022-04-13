package com.google.gdc.templates;


import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gdc.IO.IOBuilders;
import com.google.gdc.utils.SchemaParser;
import com.google.gdc.utils.Utilities;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PubSubMultiGcsFoldersNotificationToBQ {

    private PubSubMultiGcsFoldersNotificationOptions options;
    private Pipeline pipeline;

    private static final Logger LOG = LoggerFactory.getLogger(SchemaParser.class);

    public PubSubMultiGcsFoldersNotificationToBQ(){

    }

    public PubSubMultiGcsFoldersNotificationToBQ(Pipeline pipeline,
                                                 PubSubMultiGcsFoldersNotificationOptions options){
        this.options = options;
        this.pipeline = pipeline;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public PubSubMultiGcsFoldersNotificationOptions getOptions() {
        return options;
    }

    public void setOptions(PubSubMultiGcsFoldersNotificationOptions options) {
        this.options = options;
    }


    public MapElements<String, String> getFilePath(){
        return MapElements.into(TypeDescriptors.strings()).via( (String n) ->
                Utilities.extractPath(n, "gs://"));
    }

    public BigQueryIO.Write<TableRow> writeToBq(String outputTable, String outputSchemaPath) throws Exception {
        TableSchema tableSchema = SchemaParser.convertJsonToTableSchema(outputSchemaPath);
        return BigQueryIO
                .writeTableRows()
                .to(outputTable)
                .withSchema(tableSchema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);
    }


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


    public Pipeline build() throws Exception {

        String topicName = "projects/" + getOptions().getProject() + "/topics/"+ getOptions().getTopic().get();
        String jobConfigPath = getOptions().getJobConfigurationPath().get();

        JSONObject jobConfig = SchemaParser.parseSchemaFile(jobConfigPath);

        PCollection<String>  notifications = getPipeline()
                .apply("Read GCS Notifications", IOBuilders.fromPubSub(topicName));

        PCollection<String> files = notifications
                .apply("Extract File Path", getFilePath());

        for(String key: jobConfig.keySet())
            processSource(files, jobConfig.getJSONObject(key));

        return getPipeline();
    }

    private void processSource(PCollection<String> files, JSONObject sourceConfig) throws Exception {

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

        rows.apply("Write to BigQuery", writeToBq(outputTable, outputSchemaPath));
    }


}
