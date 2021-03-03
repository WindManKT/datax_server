package com.wind.entity.mysql;

import lombok.Data;

import java.util.ArrayList;

@Data
public class Connection {
    private ArrayList<String> table, jdbcUrl, querySql = new ArrayList<>();

    private boolean isEmpty(ArrayList<String> list) {
        return list.size() == 0;
    }

    public boolean connectionChecker(ArrayList<Connection> connections) {
        boolean lack = false;
        if (connections.size() == 0) {
            return lack;
        } else {
            for (Connection connection : connections) {
                if (isEmpty(connection.getJdbcUrl()) || isEmpty(connection.getTable())) {
                    lack = true;
                    break;
                }
            }
        }
        return !lack;
    }


}
