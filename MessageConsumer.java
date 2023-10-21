package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.*;
import java.util.Properties;
import java.time.Duration;
import com.fasterxml.jackson.databind.ObjectMapper;


public class MessageConsumer {
    private static void processJsonValue(String jsonValue) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            HashMap<String, String> dictionary = objectMapper.readValue(jsonValue, HashMap.class);

            // Now you can process the dictionary as needed
            System.out.println("Processed dictionary: " + dictionary);
            dictionary.remove("PHONE_NUMBER");
            MessageProducer.prod(dictionary,"second");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static  void consume_msg(String topic){
        try {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer_grp1");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            // ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1001));
            while(true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println("Key: " + consumerRecord.key() + "Value: " + consumerRecord.value());
                    processJsonValue(consumerRecord.value());
                }
            }
        }
        catch(Exception e){
            System.out.println("error in consumer:-" + e);
        }
    }
    public static void main(String[] args){
        consume_msg("first");
    }
}
