package com.github.learnkafka.kafka.Firstpackage;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKeys {
    public static void main(String[] args) {
        System.out.println("Hello World");
        //Logger
        final Logger logger = LoggerFactory.getLogger(ProducerKeys.class);

        //Producer Properties

        String bootstrapServers = "127.0.0.1:9092";
        String topicname    = "new_topic";
        String topicvalue   = "Hello";
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", bootstrapServers);
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Another way of doing these configurations is using producerconfig
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create a Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for ( int i = 0; i < 10 ; i++ )
        //Create a Producer Record to be send
        {
            topicvalue = topicvalue + " " + Integer.toString(i);
            String topicKey = "Id_" + Integer.toString(i);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>( topicname, topicKey, topicvalue);
            //Send data in the producer
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //Print the record Metadata
                        /* System.out.println(recordMetadata.offset());
                        System.out.println(recordMetadata.partition());
                        System.out.println(recordMetadata.topic());
                        System.out.println(recordMetadata.timestamp());
                        */

                        logger.info("Offset->" + recordMetadata.offset() + "\n" +
                        "Timestamp->" + recordMetadata.timestamp() + "\n"  +
                        "Partition->" + recordMetadata.partition() + "\n" +
                        "topic->" + recordMetadata.topic() + "\n");
                    } else {
                        System.out.println(e.fillInStackTrace());
                        logger.info("Error Message :" + e.getStackTrace());
                    }
                }
            });

        }
        //flush or close producer
        producer.flush();

        //close
        producer.close();
    }
}
