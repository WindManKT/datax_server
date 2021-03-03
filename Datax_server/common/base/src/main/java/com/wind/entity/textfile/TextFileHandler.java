package com.wind.entity.textfile;

import lombok.Data;

import java.util.ArrayList;

@Data
public abstract class TextFileHandler {
    private ArrayList<String> path;
    private String compress, encoding, nullFormat;
}
