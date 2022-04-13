package com.google.gdc.templates;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubSubMultiGcsFoldersNotificationOptions extends DataflowPipelineOptions {

    @Description("PubSub topic")
    ValueProvider<String> getTopic();
    void setTopic(ValueProvider<String> topic);

    @Description("Job Configuration File Path")
    ValueProvider<String> getJobConfigurationPath();
    void setJobConfigurationPath(ValueProvider<String> configuration);

}
