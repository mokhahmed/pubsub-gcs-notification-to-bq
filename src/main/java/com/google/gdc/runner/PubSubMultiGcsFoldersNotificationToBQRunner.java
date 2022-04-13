package com.google.gdc.runner;

import com.google.gdc.templates.PubSubMultiGcsFoldersNotificationOptions;
import com.google.gdc.templates.PubSubMultiGcsFoldersNotificationToBQ;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PubSubMultiGcsFoldersNotificationToBQRunner {


    public static void main(String[] args)  {

        PipelineOptionsFactory.register(PubSubMultiGcsFoldersNotificationOptions.class);
        PubSubMultiGcsFoldersNotificationOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation().as(PubSubMultiGcsFoldersNotificationOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PubSubMultiGcsFoldersNotificationToBQ runner = new PubSubMultiGcsFoldersNotificationToBQ(pipeline, options);

        try {
            runner.build().run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
