package com.google.gdc.runner;


import com.google.gdc.templates.PubSubGcsNotificationOptions;
import com.google.gdc.templates.PubSubGcsNotificationToBQ;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PubSubGcsNotificationToBQRunner {


    public static void main(String[] args)  {

        PipelineOptionsFactory.register(PubSubGcsNotificationOptions.class);
        PubSubGcsNotificationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubGcsNotificationOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PubSubGcsNotificationToBQ runner = new PubSubGcsNotificationToBQ(pipeline, options);

        try {
            runner.build().run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
