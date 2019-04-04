package com.panda.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log= LoggerFactory.getLogger(ProducerDemoCallBack.class);
    public static void main(String[] args) {
        Properties props=new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"indlin5007:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"grp1");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //default latest

        //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(props);
        //subscriber the topic
        consumer.subscribe(Arrays.asList("first_topic"));
        //poll the data
        while (true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            records.forEach(r->{
                log.info(r.key()+"_"+r.value()+"_"+r.partition()+"_"+r.offset());
            });
        }
    }
}
