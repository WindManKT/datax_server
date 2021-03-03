package com.wind.entity.textfile;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class TextFileReader extends TextFileHandler {
    private ArrayList<TextFileBlock> column;
    private Boolean skipHeader;
    private CsvReaderConfig csvReaderConfig;
    private String fieldDelimiter;

    public boolean gainSafetySwitch() {
        return csvReaderConfig.getSafetySwitch();
    }

    public boolean gainSkipEmptyRecords() {
        return csvReaderConfig.getSkipEmptyRecords();
    }

    public boolean gainUseTextQualifier() {
        return csvReaderConfig.getUseTextQualifier();
    }

    public String solution(String S) {
        char result = ' ';
        int curr = 0;
        List<Character> goodChar = new ArrayList<>();
        Map<Character, Integer> map = new HashMap<>();
        for (Character c: S.toCharArray()) {
            if (!map.containsKey(c)) {
                if (Character.isUpperCase(c)) {
                    map.put(c,1);
                } else {
                    map.put(c,0);
                }
            } else {
                int i = map.get(c);
                if ((Character.isUpperCase(c) && i == 0) || (Character.isLowerCase(c) && i == 1)) {
                    goodChar.add(c);
                }
            }
        }
        for (Character x: goodChar) {
            int temp = Character.toLowerCase(x) - 'a';
            if (temp > curr) {
                result = x;
                curr = temp;
            }
        }
        return Character.toString(result);
    }
}
