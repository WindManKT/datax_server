package com.wind.entity.mongodb;

import lombok.Data;

import java.util.ArrayList;

@Data
public abstract class MongoDBHandler {
    protected ArrayList<String> address;
    protected String userName, userPassword, collectionName, dbName;
    protected ArrayList<MongoBlock> column;
}
