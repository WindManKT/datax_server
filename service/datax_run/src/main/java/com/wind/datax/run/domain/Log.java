package com.wind.datax.run.domain;

import com.baomidou.mybatisplus.annotation.*;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
@TableName("datax_log")
public class Log  implements Serializable {
    //生成id方式
    @ApiModelProperty(value = "ID,不传")
    @TableId(type = IdType.ID_WORKER)
    private Long id;

    //dataxId
    @ApiModelProperty(value = "dataxId")
    private String dataxId;

    //日志
    @ApiModelProperty(value = "日志")
    private String log;

    //创建时间
    @TableField(fill = FieldFill.INSERT)
    @ApiModelProperty(value = "创建时间，不传")
    private Date createTime;
    //修改时间
    @TableField(fill = FieldFill.INSERT_UPDATE)
    @ApiModelProperty(value = "修改时间，不传")
    private Date updateTime;
    //版本
    @Version
    @TableField(fill = FieldFill.INSERT)
    @ApiModelProperty(value = "版本，不传")
    private Integer version;
    //逻辑删除
    @TableLogic
    @TableField(fill = FieldFill.INSERT)
    @ApiModelProperty(value = "逻辑删除，不传")
    private  Integer deleted;
}
