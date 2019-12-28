package com.github.learnkafka.kafka.Firstpackage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumeAssignSeek {
    public static void main(String[] args) {
        System.out.println("Hello World");
        Logger logger = LoggerFactory.getLogger(ConsumeAssignSeek.class.getName());
        //Producer Properties

        String bootstrapServers = "127.0.0.1:9092";
        //String groupId = "consumer-demo";
        String topicName = "new_topic";
        Long readFromOffset = 15L;
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", bootstrapServers);
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Another way of doing these configurations is using consumer config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Create a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe for the  data in the producer
        //consumer.subscribe(Arrays.asList(topicName));

        //Assign a Partition
        TopicPartition partitionToReadFrom = new TopicPartition(topicName, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Seek the values from the partition
        consumer.seek(partitionToReadFrom, readFromOffset);

        Boolean  continueReading = true;
        Integer  numberOfMessagesRead = 5;
        Integer  totalRead = 0;
        //poll the producer topic to check for any new records.
        while(continueReading) {
           ConsumerRecords<String , String> ConsumerRecord = consumer.poll(Duration.ofMillis(100));
            for ( ConsumerRecord<String, String> record: ConsumerRecord) {
                logger.info("\n record offset :" + record.offset() + "\n" + " key:" + record.key() + "\n" +
                        " topic :" + record.topic() + "\n Value " + record.value() + "\n Partition" + record.partition());

                totalRead += 1;
                if (totalRead > numberOfMessagesRead) {
                    logger.info("Total Number of Messages Read exceed the limitation.");
                    break;
                }
            }
        }
    }
}
