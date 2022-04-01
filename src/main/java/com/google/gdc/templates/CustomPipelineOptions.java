package com.google.gdc.templates;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface CustomPipelineOptions extends DataflowPipelineOptions {

    @Description("PubSub topic")
    ValueProvider<String> getTopic();
    void setTopic(ValueProvider<String> topic);

    @Description("BQ table name")
    ValueProvider<String>  getBqTable();
    void setBqTable(ValueProvider<String> bqTable);

    @Description("Files Format")
    ValueProvider<String> getFilesFormat();
    void setFilesFormat(ValueProvider<String> format);

    @Description("Files Schema")
    ValueProvider<String> getSchema();
    void setSchema(ValueProvider<String> schema);


}
