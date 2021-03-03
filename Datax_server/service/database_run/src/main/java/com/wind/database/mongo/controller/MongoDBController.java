package com.wind.database.mongo.controller;

import com.wind.database.mongo.service.MongoDBService;
import com.wind.doamin.R;
import com.wind.entity.mongodb.MongoDBReader;
import com.wind.entity.mongodb.MongoDBWriter;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin
public class MongoDBController {

    @Autowired
    MongoDBService mongoDBService;

    @PostMapping("/readMongo")
    public R readMongo(@RequestBody MongoDBReader mongoDBReader) {
        JSONObject result = mongoDBService.readMongo(mongoDBReader);
        return (result == null) ? R.error().data("error", "参数缺失") : R.ok().data("json", result);
    }

    @PostMapping("/writeMongo")
    public R writeMongo(@RequestBody MongoDBWriter mongoDBWriter) {
        JSONObject result = mongoDBService.writeMongo(mongoDBWriter);
        return (result == null) ? R.error().data("error", "参数缺失") : R.ok().data("json", result);
    }
}
