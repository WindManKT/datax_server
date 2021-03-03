package com.wind.base.handler;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import org.apache.ibatis.reflection.MetaObject;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class NoloseObjectHandler implements MetaObjectHandler {
    @Override
    public void insertFill(MetaObject metaObject) {
        this.setFieldValByName("gmtCreate",new Date(),metaObject);
        this.setFieldValByName("gmtModified",new Date(),metaObject);

        //传教时间
        this.setFieldValByName("createTime",new Date(),metaObject);
        //修改时间
        this.setFieldValByName("updateTime",new Date(),metaObject);
        //版本默认值
        this.setFieldValByName("version",1,metaObject);
        //删除标识默认值
        this.setFieldValByName("deleted",0,metaObject);
    }

    @Override
    public void updateFill(MetaObject metaObject) {
        this.setFieldValByName("gmtModified",new Date(),metaObject);

        this.setFieldValByName("updateTime",new Date(),metaObject);
    }
}
