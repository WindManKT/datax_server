package com.wind.entity.transformer;

import lombok.Data;

import java.util.Map;

@Data
public abstract class UDF {
    protected String type;
    protected Map object;


//    protected abstract void setType();
//    protected abstract void printType();
}
