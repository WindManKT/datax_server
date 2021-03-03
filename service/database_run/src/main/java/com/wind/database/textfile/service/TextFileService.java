package com.wind.database.textfile.service;

import com.wind.entity.textfile.TextFileReader;
import com.wind.entity.textfile.TextFileWriter;
import net.sf.json.JSONObject;

public interface TextFileService {
    JSONObject readTextFile(TextFileReader textFileReader);

    JSONObject writeTextFile(TextFileWriter textFileWriter);
}
