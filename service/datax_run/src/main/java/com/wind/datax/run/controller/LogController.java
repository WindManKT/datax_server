package com.wind.datax.run.controller;

import com.wind.datax.run.domain.Log;
import com.wind.datax.run.service.LogService;
import com.wind.doamin.R;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/log")
@CrossOrigin
public class LogController {

    @Autowired
    LogService logService;

    @PostMapping("/list")
    public R insert(@RequestBody Log log){
        return logService.save(log)?R.ok():R.error();
    }
}
