package com.wind.task.run.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.wind.entity.task.Task;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface TaskMapper extends BaseMapper<Task> {
}
