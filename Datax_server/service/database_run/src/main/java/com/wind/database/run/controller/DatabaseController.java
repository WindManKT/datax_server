package com.wind.database.run.controller;

import com.wind.database.run.client.TaskClient;
import com.wind.database.run.service.DatabasesService;
import com.wind.doamin.R;
import com.wind.entity.task.Task;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@RestController
@RequestMapping("/database")
@CrossOrigin
public class DatabaseController {
    @Autowired
    DatabasesService databasesService;

    @Autowired
    TaskClient taskClient;


    @PostMapping("/carried")
    public R carried(@RequestBody JSONObject jsonObject){
        Long id = jsonObject.getLong("id");
        String dataxId = jsonObject.getString("dataxId");
        Task task = taskClient.getTask(id);
        System.err.println(task);
        JSONObject jsonObject1 = JSONObject.fromObject(task.getJob());
        System.err.println(jsonObject1);
//        TaskVo task = (TaskVo) taskClient.getTask(id).getData().get("task");
//        JSONObject jobs = JSONObject.fromObject(task.getJob());
//        jobs.put("dataxId",dataxId);
        return R.ok();
    }

    @PostMapping("/runDatabase")
    public R runDatabase(@RequestBody JSONObject jsonObject){
        JSONArray jobs =  jsonObject.getJSONArray("jobs");
        String dataxId = jsonObject.getString("dataxId");
        String desc = jsonObject.getString("desc");
        BlockingQueue queue = new LinkedBlockingDeque();

        jobs.forEach(job->queue.offer(job));

        try {
            databasesService.runJob(queue,dataxId,desc);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        return R.ok().data("data","已存入请求队列").data("dataxID",dataxId);
    }


    @PostMapping("/toJob")
    public R toJob(@RequestBody List<JSONObject> list){
        JSONObject job = new JSONObject();
        JSONObject content = new JSONObject();
        content.put("content",list);
        job.put("job",content);
        return R.ok().data("json",job);
    }
}
