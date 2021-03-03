package com.wind.entity.textfile;

import lombok.Data;

import java.util.ArrayList;

@Data
public class TextFileWriter extends TextFileHandler {
    private String fileName, writeMode, dateFormat, fileFormat, fieldDelimiter;
    private ArrayList<String> header;
}
