package com.wind.entity.mongodb;

import com.wind.utils.database.DatabaseUtil;
import lombok.Data;

import java.util.ArrayList;

@Data
public class MongoBlock {
    private String name, type, splitter;

    public boolean columnChecker(ArrayList<MongoBlock> mongoBlocks) {
        if (mongoBlocks.size() == 0) {
            return false;
        } else {
            for (MongoBlock b: mongoBlocks) {
                if (!(DatabaseUtil.stringChecker(b.getName()))) {
                    return false;
                }
            }
            return true;
        }
    }
}
