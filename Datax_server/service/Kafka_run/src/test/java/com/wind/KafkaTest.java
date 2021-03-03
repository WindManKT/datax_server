package com.wind;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTest {

    @Test
    public void producer() throws InterruptedException, ExecutionException {
        // 设置属性
        Properties props = new Properties();
        // 设置键的类型，实际上是偏移量
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        // 设置值的类型，实际上是实际数据
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 设置Kafka的连接地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.41.129:9092");
        // 添加数据
        Producer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<Integer, String> message = new ProducerRecord<Integer, String>("test", "" + i);
            System.err.println("i==" + i);
            kafkaProducer.send(message);
            System.err.println("i==" + i+"===");
        }
        while (true){
            Thread.sleep(1000);
            System.err.println("???");
        }
    }

    @Test
    public void consumer_1() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.41.130:9092");
        props.put("group.id", "test01");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records)
                System.err.println("c1消费:" + record.offset() + ":" + record.value());
            }
        } catch (Exception e) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void consumer_2() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "112.74.195.201:9092");
        props.put("group.id", "consumer-tutorial");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("enbook", "t2"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records)
                    System.out.println("c2消费:" + record.offset() + ":" + record.value());
            }
        } catch (Exception e) {
        } finally {
            consumer.close();
        }
    }


}
