package com.google.gdc.IO;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

public class IOBuilders {

    public static  TextIO.Read fromFile(String path){
        return TextIO.read().from(path);
    }



    public  static PubsubIO.Read<String> fromPubSub(String topicName){
        return PubsubIO.readStrings().fromTopic(topicName);
    }

}
