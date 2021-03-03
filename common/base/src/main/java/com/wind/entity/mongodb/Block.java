package com.wind.entity.mongodb;

import com.wind.utils.database.DatabaseUtil;
import lombok.Data;

import java.util.ArrayList;

@Data
public class Block {
    private String name, type, splitter;

    public boolean blockChecker(ArrayList<Block> blocks) {
        if (blocks.size() == 0) {
            return false;
        } else {
            for (Block b: blocks) {
                if (!(DatabaseUtil.stringChecker(b.getName()))) {
                    return false;
                }
            }
            return true;
        }
    }
}
