package com.panda.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoCallBack {
    private static final Logger log= LoggerFactory.getLogger(ProducerDemoCallBack.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //create the prod props
        Properties props=new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"indlin5007:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the prod
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(props);

        //Create record
        final ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","1","hello Kafka");
        //send the data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes if send successful or execetion
                if(e!=null){
                    log.info("metadata recieved"+ recordMetadata.topic() +"-"+recordMetadata.timestamp()+"_"+recordMetadata.offset()
                    +"_"+recordMetadata.partition());
                }else{
                    log.error("error in sending data "+e);
                }
            }
        }).get();// block the producer until record is send // synchronous //bad practice

        // flush data
        producer.flush();
        //producer close
        producer.close();

    }
}
