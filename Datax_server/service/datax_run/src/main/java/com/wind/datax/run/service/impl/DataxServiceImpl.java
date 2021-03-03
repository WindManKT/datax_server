package com.wind.datax.run.service.impl;

import com.wind.base.exception.NoloseException;
import com.wind.datax.run.client.KafkaClient;
import com.wind.datax.run.util.Engine;
import com.wind.doamin.ResultCode;
import com.wind.datax.run.domain.Setting;
import com.wind.datax.run.service.DataxService;
import com.wind.datax.run.util.DataxUtil;
import com.wind.utils.json.JsonUtil;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.util.parsing.combinator.testing.Str;

import java.util.*;

@Service
public class DataxServiceImpl implements DataxService {
    @Value("${Datax.path}")
    private String filePath;
    @Value("${Datax.name}")
    private String fileName;
    @Value("${Datax.home}")
    private String home;

//    @Value("${Datax.jsonPath}")
//    private String jsonPath;

    @Autowired
    private Setting settingBean;

    @Autowired
    private KafkaClient kafkaClient;

    @Override
    public synchronized void toJson(JSONObject jsonObject) {

        try {
            String dataxId = jsonObject.getString("dataxId");
            String desc = jsonObject.getString("desc");

            Engine.DATAX_ID = dataxId;
            Engine.DESC = desc;
        }catch (Exception e){}

        jsonObject.remove("dataxId");
        jsonObject.remove("desc");

        if (!JsonUtil.createJsonFile(jsonObject.toString(), filePath, fileName)) throw new NoloseException(ResultCode.ERROR,"Json创建失败");

        try {
            DataxUtil.runJson(home, filePath+"/"+fileName+".json");
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw new NoloseException(ResultCode.ERROR,throwable.getMessage());
        }
    }

    @Override
    public JSONObject toSetting() {
        JSONObject setting = new JSONObject();
        JSONObject speed = new JSONObject();
        JSONObject errorLimit = new JSONObject();


        speed.put("channel",settingBean.getSpeed_channel());
//        speed.put("byte",settingBean.getSpeed_byte());
//        speed.put("record",settingBean.getSpeed_record());

        errorLimit.put("record",settingBean.getErrorLimit_record());
        errorLimit.put("percentage",settingBean.getErrorLimit_percentage());

        setting.put("speed",speed);
        setting.put("errorLimit",errorLimit);

        return setting;
    }
}
