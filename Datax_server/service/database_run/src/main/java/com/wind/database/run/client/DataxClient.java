package com.wind.database.run.client;

import com.wind.doamin.R;
import net.sf.json.JSONObject;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Component
@FeignClient(name = "datax-internal-dev")
public interface DataxClient {

    @PostMapping("/run/runJson")
    R toJson(@RequestBody JSONObject jsonObject);

    @PostMapping("/run/toSetting")
    R toSetting();
}
