package com.wind.entity.mongodb;

import lombok.Data;

@Data
public class UpsertInfo {
    private String isUpsert, upsertKey;
}
