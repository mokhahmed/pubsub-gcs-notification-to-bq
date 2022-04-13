package com.google.gdc.templates;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubSubGcsNotificationOptions extends DataflowPipelineOptions {

    @Description("PubSub topic")
    ValueProvider<String> getTopic();
    void setTopic(ValueProvider<String> topic);

    @Description("BQ output table name")
    ValueProvider<String>  getBqTable();
    void setBqTable(ValueProvider<String> bqTable);

    @Description("Input Files Format")
    ValueProvider<String> getInputFilesFormat();
    void setInputFilesFormat(ValueProvider<String> inputFilesFormat);

    @Description("Input Schema")
    ValueProvider<String> getInputSchema();
    void setInputSchema(ValueProvider<String> inputSchema);

    @Description("Output Schema")
    ValueProvider<String> getOutputSchema();
    void setOutputSchema(ValueProvider<String> outputSchema);

    @Description("File Delimiter")
    ValueProvider<String> getFileDelimiter();
    void setFileDelimiter(ValueProvider<String> fileDelimiter);

}
