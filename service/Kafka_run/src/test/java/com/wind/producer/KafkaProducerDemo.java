package com.wind.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerDemo {

    public static Properties kafkaProperties() {

        Properties properties = new Properties();

        /*设置集群kafka的ip地址和端口号*/
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "112.74.195.201:9092");
        properties.put("bootstrap.servers", "112.74.195.201:9092");
        /*发送的消息需要leader确认*/
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        /*用户id*/
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerDemo");
        /*对key进行序列化*/
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerSerializer");
        /*对value进行序列化*/
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        return properties;
    }

    public static void main(String[] args) throws InterruptedException {
        /*创建一个kafka生产者*/
        KafkaProducer<Integer, String> kafkaProducer =
                new KafkaProducer<Integer, String>(kafkaProperties());
        /*主题*/
        String topic = "test";
        /*循环发送数据*/
        for (int i = 0; i < 20; i++) {
            /*发送的消息*/
            String message = "我是一条信息" + i;
            /*发出消息*/
            System.err.println(kafkaProducer.toString());
            kafkaProducer.send(new ProducerRecord<>(topic, message));
            System.out.println(message + "->已发送");
            Thread.sleep(1000);
        }

    }
}
