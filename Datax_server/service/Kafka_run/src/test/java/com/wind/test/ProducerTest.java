package com.wind.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerTest {


    public static void main(String[] args) {

        Properties props = new Properties(); //读取配置文件
        props.put("bootstrap.servers", "http://192.168.41.129:9092");

        //设置为all将导致记录的完整提交阻塞，最慢的，但最持久的设置。
        props.put("acks", "all");
        //如果请求失败，生产者也会自动重试，即使设置成０
        props.put("retries", 0);

        props.put("batch.size", 16384);
        // 默认立即发送，这里这是延时毫秒数
        props.put("linger.ms", 1);
        // 生产者缓冲大小，当缓冲区耗尽后，额外的发送调用将被阻塞。时间超过max.block.ms将抛出TimeoutException
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建kafka的生产者类
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        // 生产者的主要方法
        producer.send(new ProducerRecord<String, String>("test", "测试Kafka"));

        producer.close();

    }

}
