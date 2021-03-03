package com.wind.database.run.service.impl;

import com.wind.database.run.client.DataxClient;
import com.wind.database.run.service.DatabasesService;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

@Service
public class DatabasesServiceImpl implements DatabasesService {

    @Autowired
    private DataxClient dataxClient;

    @Autowired
    ExecutorService executorService;

    @Override
    public void runJob(BlockingQueue queue, String dataxId,String desc) throws InterruptedException {
        queue.forEach(q->{
            JSONObject job = (JSONObject) q;
            JSONObject jobOR = job.getJSONObject("job");
            jobOR.put("setting", dataxClient.toSetting().getData().get("setting"));
            job.put("job",jobOR);
            job.put("dataxId",dataxId);
            job.put("desc",desc);

            executorService.execute(new Runnable() {
                public void run() {
                   try {
                       dataxClient.toJson(job);
                   }catch (Exception e){
                       System.err.println(e.getMessage());
                   }
                }
            });
        });
    }
}
