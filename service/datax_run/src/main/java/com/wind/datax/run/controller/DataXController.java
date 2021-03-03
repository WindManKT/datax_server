package com.wind.datax.run.controller;

import com.wind.base.exception.NoloseException;
import com.wind.datax.run.service.DataxService;
import com.wind.doamin.R;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/run")
@CrossOrigin
public class DataXController {

    @Autowired
    private DataxService dataxService;

    @PostMapping("/runJson")
    public R toJson(@RequestBody JSONObject jsonObject){
        try {
            dataxService.toJson(jsonObject);
            return R.ok().data("json",jsonObject);
        }catch (NoloseException e){
            return R.error().data("错误",e.getMsg());
        }
    }

    @PostMapping("/toSetting")
    public R toSetting(){
        JSONObject jsonObject = dataxService.toSetting();
        return R.ok().data("setting",jsonObject);
    }

}
