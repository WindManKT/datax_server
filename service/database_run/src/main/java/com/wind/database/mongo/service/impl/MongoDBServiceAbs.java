package com.wind.database.mongo.service.impl;

import com.wind.base.exception.NoloseException;
import com.wind.doamin.ResultCode;
import com.wind.entity.mongodb.Block;
import com.wind.entity.mongodb.MongoBlock;
import com.wind.entity.mongodb.MongoDBHandler;
import com.wind.utils.database.DatabaseUtil;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public abstract class MongoDBServiceAbs {
    public JSONObject mongoProcessor(MongoDBHandler mongoDBHandler, boolean isReader) {
        MongoBlock mongoBlock = new MongoBlock();

        boolean addressProvided = DatabaseUtil.listChecker(mongoDBHandler.getAddress());
        boolean userNameProvided = DatabaseUtil.stringChecker(mongoDBHandler.getUserName());
        boolean passwordProvided = DatabaseUtil.stringChecker(mongoDBHandler.getUserPassword());
        boolean dbNameProvided = DatabaseUtil.stringChecker(mongoDBHandler.getDbName());
        boolean collectionNameProvided = DatabaseUtil.stringChecker(mongoDBHandler.getCollectionName());
        boolean columnProvided = mongoBlock.columnChecker(mongoDBHandler.getColumn());

        if (!addressProvided) {
            throw new NoloseException(ResultCode.ERROR, "address参数缺失");
        } else if (!collectionNameProvided) {
            throw new NoloseException(ResultCode.ERROR, "connectionName参数缺失");
        } else if (!columnProvided) {
            throw new NoloseException(ResultCode.ERROR, "column参数缺失");
        } else if (dbNameProvided) {
            JSONObject result = new JSONObject();
            JSONObject handler = new JSONObject();
            JSONObject parameter = new JSONObject();

            List<JSONObject> blocksJson = new ArrayList<>();
            List<MongoBlock> column = mongoDBHandler.getColumn();

            for (MongoBlock b: column) {
                JSONObject blockJson = new JSONObject();
                blockJson.put("name", b.getName());
                if (DatabaseUtil.stringChecker(b.getType())) {
                    blockJson.put("type", b.getType());
                }
                if (DatabaseUtil.stringChecker(b.getSplitter())) {
                    blockJson.put("splitter", b.getSplitter());
                }
                blocksJson.add(blockJson);
            }

            if (userNameProvided) {
                parameter.put("userName", mongoDBHandler.getUserName());
            }
            if (passwordProvided) {
                parameter.put("userPassword", mongoDBHandler.getUserPassword());
            }

            parameter.put("address", mongoDBHandler.getAddress());
            parameter.put("dbName", mongoDBHandler.getDbName());
            parameter.put("collectionName", mongoDBHandler.getCollectionName());

            parameter.put("column", blocksJson);

            handler.put("name", isReader ? "mongodbreader":"mongodbwriter");
            handler.put("parameter", parameter);
            result.put(isReader ? "reader":"writer", handler);

            return result;
        } else {
            throw new NoloseException(ResultCode.ERROR, "dbName参数缺失");
        }
    }
}
