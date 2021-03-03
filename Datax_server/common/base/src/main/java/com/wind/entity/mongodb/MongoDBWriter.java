package com.wind.entity.mongodb;

import lombok.Data;

@Data
public class MongoDBWriter extends MongoDBHandler {
    private UpsertInfo upsertInfo;
}
