package com.wind.datax.run.client;

import com.wind.doamin.R;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.HashMap;
import java.util.List;

@Component
@FeignClient(name = "datax-kafka-dev")
public interface KafkaClient {

    @PostMapping("/kafka/saveData")
    public R saveData(@RequestBody HashMap<String, List<String>> o);

}
