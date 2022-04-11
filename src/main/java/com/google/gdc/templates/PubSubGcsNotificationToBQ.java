package com.google.gdc.templates;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gdc.IO.IOBuilders;
import com.google.gdc.utils.SchemaParser;
import com.google.gdc.utils.Utilities;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;


public class PubSubGcsNotificationToBQ {

    private CustomPipelineOptions options;
    private Pipeline pipeline;

    private static final Logger LOG = LoggerFactory.getLogger(SchemaParser.class);

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

        String topicName = "projects/" + getOptions().getProject() + "/topics/"+ getOptions().getTopic().toString();
        ValueProvider<String> inputSchemaPath = getOptions().getInputSchema();
        ValueProvider<String> outputTable = getOptions().getBqTable();
        ValueProvider<String> outputSchemaPath = getOptions().getOutputSchema();
        ValueProvider<String> fileDelimiter = getOptions().getFileDelimiter();

        PCollection<String>  notifications = getPipeline().apply("Read GCS Notifications",
                IOBuilders.fromPubSub(topicName));

        PCollection<String> files = notifications.apply("Extract File Path",
                getFilePath());

        PCollection<String> contents = files.apply("Read Files", FileIO.matchAll())
                .apply(FileIO.readMatches())
                .apply(TextIO.readFiles());

        PCollection<TableRow> rows = contents.apply("To BQ Row",
                stringToBqRow(inputSchemaPath.get(),fileDelimiter.get() ));

        rows.apply("Write to BigQuery",
                writeToBq(outputTable.get(), outputSchemaPath.get()));

         return getPipeline();
    }


}
