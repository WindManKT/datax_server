package com.wind.task.run.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.wind.entity.task.Task;
import com.wind.task.run.mapper.TaskMapper;
import com.wind.task.run.service.TaskService;
import org.springframework.stereotype.Service;

@Service
public class TaskServiceImpl extends ServiceImpl<TaskMapper, Task> implements TaskService {
}
