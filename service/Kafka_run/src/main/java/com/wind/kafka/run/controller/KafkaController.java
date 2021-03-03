package com.wind.kafka.run.controller;


import com.wind.doamin.R;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/kafka")
@CrossOrigin
public class KafkaController {

    @Autowired
    KafkaProducer kafkaProducer;

    @Autowired
    KafkaConsumer kafkaConsumer;

    @Autowired
    List list;

    @PostMapping("/saveData")
    public R saveData(@RequestBody HashMap<String, List<String>> o){
        o.forEach((key,value)->value.forEach(v->kafkaProducer.send(new ProducerRecord<>(key,v))));
        return R.ok();
    }

    @PostMapping("/fetchData")
    public R fetchData(@RequestBody String topic){
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = kafkaConsumer.poll(2000);
        records.forEach(r->list.add(r.offset() + "|" + r.value()));
        return R.ok().data("kafka",list);
    }
}
