package com.google.gdc.templates;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface CustomPipelineOptions extends DataflowPipelineOptions {

    @Description("PubSub topic")
    String getTopic();
    void setTopic(String topic);

    @Description("BQ table name")
    String getBqTable();
    void setBqTable(String bqTable);

    @Description("Files Format")
    String getFilesFormat();
    void setFilesFormat(String format);

    @Description("Files Schema")
    String getSchema();
    void setSchema(String schema);


}
