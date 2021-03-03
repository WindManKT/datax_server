package com.wind.datax.run.domain;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Data
@Component
public class Setting {
    @Value("${speed.channel}")
    private Integer speed_channel;
    @Value("${speed.byte}")
    private Integer speed_byte;
    @Value("${speed.record}")
    private Integer speed_record;
    @Value("${errorLimit.record}")
    private String errorLimit_record;
    @Value("${errorLimit.percentage}")
    private String errorLimit_percentage;
}
