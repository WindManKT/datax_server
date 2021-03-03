package com.wind.entity.mysql;

import com.wind.entity.mysql.MySQLHandler;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;

@Data
public class MySQLReader extends MySQLHandler {
    protected ArrayList<String> where;
    protected String splitPK;
}
