package com.wind.entity.textfile;

import com.wind.utils.database.DatabaseUtil;
import lombok.Data;

import java.util.ArrayList;

@Data
public class TextFileBlock {
    private Integer index;
    private String type, format;

    public boolean columnExists(ArrayList<TextFileBlock> textFileBlocks) {
        return textFileBlocks.size() != 0;
    }

    public boolean columnValid(ArrayList<TextFileBlock> textFileBlocks) {
        for (TextFileBlock b: textFileBlocks) {
            if (b.getIndex() == null || !DatabaseUtil.stringChecker(b.getType())) {
                return false;
            }
        }
        return true;
    }
}
