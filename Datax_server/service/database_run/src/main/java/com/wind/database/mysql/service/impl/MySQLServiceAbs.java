package com.wind.database.mysql.service.impl;

import com.wind.base.exception.NoloseException;
import com.wind.doamin.ResultCode;
import com.wind.entity.mysql.MySQLHandler;
import com.wind.entity.mysql.Connection;
import com.wind.utils.database.DatabaseUtil;
import net.sf.json.JSONObject;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public abstract class MySQLServiceAbs {

    public JSONObject sqlProcessor(MySQLHandler mySQLHandler, boolean hasQuerySQL) {
        Connection connection = new Connection();

        boolean passwordProvided = DatabaseUtil.stringChecker(mySQLHandler.getPassword());
        boolean nameProvided = DatabaseUtil.stringChecker(mySQLHandler.getUsername());
        boolean columnProvided = DatabaseUtil.listChecker(mySQLHandler.getColumn());
        boolean tableAndURLProvided = connection.connectionChecker(mySQLHandler.getConnection());

        if (!passwordProvided) {
            throw new NoloseException(ResultCode.ERROR, "password参数缺失");
        } else if (!nameProvided) {
            throw new NoloseException(ResultCode.ERROR, "uerName参数缺失");
        } else if (!columnProvided) {
            throw new NoloseException(ResultCode.ERROR, "column参数缺失");
        } else if (tableAndURLProvided) {
            JSONObject result = new JSONObject();
            JSONObject handler = new JSONObject();
            JSONObject parameter = new JSONObject();

            List<JSONObject> connectionsJson = new ArrayList<>();
            List<Connection> connections = mySQLHandler.getConnection();

            for (Connection c : connections) {
                JSONObject collectionJson = new JSONObject();
                collectionJson.put("table", c.getTable());
                collectionJson.put("jdbcUrl", c.getJdbcUrl());
                if (hasQuerySQL && c.getQuerySql().size() != 0) {
                    collectionJson.put("querySql", c.getQuerySql());
                }
                connectionsJson.add(collectionJson);
            }

            parameter.put("username", mySQLHandler.getUsername());
            parameter.put("password", mySQLHandler.getPassword());
            parameter.put("column", mySQLHandler.getColumn());

            parameter.put("connection", connectionsJson);

            handler.put("name", hasQuerySQL ? "mysqlreader" : "mysqlwriter");
            handler.put("parameter", parameter);
            result.put(hasQuerySQL ? "reader" : "writer", handler);

            return result;
        } else {
            throw new NoloseException(ResultCode.ERROR, "table或jdbcUrl参数缺失");
        }
    }
}
