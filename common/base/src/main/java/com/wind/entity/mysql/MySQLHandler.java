package com.wind.entity.mysql;

import lombok.Data;

import java.util.ArrayList;

@Data
public abstract class MySQLHandler {
    protected String password, username;
    protected ArrayList<Connection> connection;
    protected ArrayList<String> column;
}
