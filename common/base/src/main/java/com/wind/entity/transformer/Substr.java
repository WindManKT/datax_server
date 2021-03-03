package com.wind.entity.transformer;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Substr extends UDF {
//    @Override
//    public void setType() {
//        type = "Substr";
//    }

    private int columnIndex;
    private List<String> paras = new ArrayList<>();


    //    @Override
//    public void printType() {
//        type = "Substr";
//    }
}
