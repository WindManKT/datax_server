package com.wind.entity.mysql;

import com.wind.entity.mysql.MySQLHandler;
import lombok.Data;

import java.util.ArrayList;

@Data
public class MySQLWriter extends MySQLHandler {
    private String writeMode;
    private ArrayList<String> session, preSql, postSql;
    private int batchSize;
}
