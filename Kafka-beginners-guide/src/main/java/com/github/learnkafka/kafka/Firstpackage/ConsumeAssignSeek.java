package com.github.learnkafka.kafka.Firstpackage;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        System.out.println("Hello World");
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        //Producer Properties

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer-demo";
        String topicName = "new_topic";
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", bootstrapServers);
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Another way of doing these configurations is using producerconfig
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Create a Producer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe for the  data in the producer
        consumer.subscribe(Arrays.asList(topicName));

        //poll the producer topic to check for any new records.
        while(true) {
           ConsumerRecords<String , String> ConsumerRecord = consumer.poll(Duration.ofMillis(100));
            for ( ConsumerRecord<String, String> record: ConsumerRecord) {
                logger.info("\n record offset :" + record.offset() +"\n" + " key:" + record.key() + "\n" +
                        " topic :" + record.topic() + "\n Value " + record.value() + "\n Partition" + record.partition());
            }
        }
    }
}
