package com.github.learnkafka.kafka.Firstpackage;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello World");

        //Producer Properties

        String bootstrapServers = "127.0.0.1:9092";
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


        //Create a Producer Record to be send
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("datatopic", "Hello Data topic");

        //Send data in the producer
        producer.send(producerRecord);


        //flush or close producer
        producer.flush();

        //close the producer
        producer.close();
    }
}
