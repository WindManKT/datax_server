package com.wind.database.transformer.controller;

import com.wind.database.transformer.service.TransformerService;
import com.wind.doamin.R;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@CrossOrigin
public class TransformerController {

    @Autowired
    TransformerService transformerService;

    @PostMapping("/handleTransformer")
    public R readMongo(@RequestBody List<JSONObject> jsonObjects) {
        JSONObject result = transformerService.handleTransformer(jsonObjects);
        return (result == null) ? R.error().data("error", "参数缺失") : R.ok().data("json", result);
    }

}
