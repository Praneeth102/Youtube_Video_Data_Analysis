package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;


public class MessageProducer {
    public static void prod(HashMap<String, String> value,String topic){
        Properties properties =new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try {
            KafkaProducer<String, String> kafkaproducer = new KafkaProducer<>(properties);
            String message = new ObjectMapper().writeValueAsString(value);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);

            kafkaproducer.send(producerRecord);
            kafkaproducer.flush();
            kafkaproducer.close();
            System.out.println("Completed");
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void main(String[] args){

    }
}
