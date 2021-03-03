package com.wind.database.mysql.controller;

import com.wind.database.run.client.DataxClient;
import com.wind.database.mysql.service.MySQLService;
import com.wind.doamin.R;
import com.wind.entity.mysql.MySQLReader;
import com.wind.entity.mysql.MySQLWriter;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin
@RequestMapping
public class MysqlController {

    @Autowired
    MySQLService service;

    @Autowired
    DataxClient dataxClient;

    @PostMapping("/readSQL")
    public R readSQL(@RequestBody MySQLReader mySQLReader) {
        JSONObject result = service.readSQL(mySQLReader);
        return (result == null) ? R.error().data("error", "参数缺失") : R.ok().data("json", result);
    }
    @PostMapping("/writeSQL")
    public R writeSQL(@RequestBody MySQLWriter mySQLWriter) {
        JSONObject result = service.writeSQL(mySQLWriter);
        return (result == null) ? R.error().data("error", "参数缺失") : R.ok().data("json", result);
    }

    @PostMapping("/test")
    public R test(){
        return dataxClient.toSetting();
    }
}
