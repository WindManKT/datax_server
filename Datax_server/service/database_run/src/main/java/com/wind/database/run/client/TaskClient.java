package com.wind.database.run.client;

import com.wind.doamin.R;
import com.wind.entity.task.Task;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
@Component
@FeignClient(name = "datax-task-dev")
public interface TaskClient {

    @PostMapping("task/getTask")
    public Task getTask(Long id);
}
