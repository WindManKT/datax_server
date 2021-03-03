package com.wind.database.mongo.service;

import com.wind.entity.mongodb.MongoDBReader;
import com.wind.entity.mongodb.MongoDBWriter;
import net.sf.json.JSONObject;

public interface MongoDBService {
    JSONObject readMongo(MongoDBReader mongoDBReader);

    JSONObject writeMongo(MongoDBWriter mongoDBWriter);
}
