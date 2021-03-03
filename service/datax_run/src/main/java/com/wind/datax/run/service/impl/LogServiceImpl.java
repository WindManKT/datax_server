package com.wind.datax.run.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.wind.datax.run.domain.Log;
import com.wind.datax.run.mapper.LogMapper;
import com.wind.datax.run.service.LogService;
import org.springframework.stereotype.Service;

@Service
public class LogServiceImpl extends ServiceImpl<LogMapper, Log> implements LogService {
}
