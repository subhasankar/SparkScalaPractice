package com.panda.demo.kafkatwits;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private static Logger logger= LoggerFactory.getLogger(TwitterProducer.class);
    String consumerKey="N9iqXvphmP2koYT0X6UXRIUPd";
    String consumerSecreat="KrhC6CWRjHc7ql3oBcnG22xWUNimPi1vKfWMZiry06BzhZwQyh";
    String token="162003201-khA3Uggtzwy9KjE5HkEOnJTvXiFycc0dg9sWQ3W4";
    String secreat="q85AD5X7z3ovh5bWFLXDZiIcDzC0rNcWMSFOVJQmTCnaU";
    public static void main(String[] args) {
        logger.info("starting app");
      new TwitterProducer().run();
    }
    public void run(){
        System.setProperty("java.net.useSystemProxies", "true");

        System.setProperty("https.proxyHost","genproxy.corp.amdocs.com");
        System.setProperty("https.proxyPort","443");
        // Attempts to establish a connection.
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Client client=createTwitterClient(msgQueue);
        client.connect();
        //create kafka producer
        KafkaProducer<String,String> kafkaProducer=createKafkaProducer();
        //link together
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("stopping client");
            client.stop();
            logger.info("closing kafka");
            kafkaProducer.close();
            logger.info("application stopped");
        }));
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_test", null,msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("something is not working");
                        }
                    }
                });
            }

        }

    }

    private KafkaProducer<String,String> createKafkaProducer() {
        String bsServer="indlin5007:9092";
        Properties props=new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bsServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        props.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        //high throughput settings
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        //create the prod
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(props);
        return producer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("Pulwama");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecreat, token, secreat);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .proxy("genproxy.corp.amdocs.com",80)
                .processor(new StringDelimitedProcessor(msgQueue));                        // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();


        return hosebirdClient;

    }
}
