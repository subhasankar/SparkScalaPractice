package com.panda.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //create the prod props
        Properties props=new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"indlin5007:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the prod
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(props);

        //Create record
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","hello Kafka");
        //send the data
        producer.send(record);
        //flush data
        producer.flush();
        //producer close
        producer.close();

    }
}
