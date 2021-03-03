package com.wind.kafka.run.bean;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Value("${kafka.url}")
    String url;

    @Bean
    public KafkaProducer getKafkaProducer() {
        Properties properties = new Properties();

        /*设置集群kafka的ip地址和端口号*/
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "local:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        /*发送的消息需要leader确认*/
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        /*用户id*/
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaConfig");
        /*对key进行序列化*/
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerSerializer");
        /*对value进行序列化*/
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        /*创建一个kafka生产者*/
        return new KafkaProducer<Integer, String>(properties);
    }

    @Bean
    public KafkaConsumer getKafkaConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", url);
        props.put("group.id", "datax-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    @Bean
    public List getKafkaList(){
        return new ArrayList();
    }
}
