package com.google.gdc.runner;


import com.google.gdc.templates.CustomPipelineOptions;
import com.google.gdc.templates.PubSubGcsNotificationToBQ;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PubSubGcsNotificationToBQRunner {


    public static void main(String[] args) {

        PipelineOptionsFactory.register(CustomPipelineOptions.class);
        CustomPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PubSubGcsNotificationToBQ runner = new PubSubGcsNotificationToBQ(pipeline, options);

        runner.build().run();
    }

}
