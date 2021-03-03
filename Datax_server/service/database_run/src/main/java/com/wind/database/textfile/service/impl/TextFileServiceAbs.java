package com.wind.database.textfile.service.impl;

import com.wind.base.exception.NoloseException;
import com.wind.doamin.ResultCode;
import com.wind.entity.textfile.TextFileHandler;
import com.wind.utils.database.DatabaseUtil;
import net.sf.json.JSONObject;
import org.springframework.stereotype.Service;

@Service
public abstract class TextFileServiceAbs {

    public JSONObject textFileProcessor(TextFileHandler textFileHandler, boolean isReading) {
        String encoding = "utf-8";
        String nullFormat = "/N";

        boolean pathProvided = DatabaseUtil.listChecker(textFileHandler.getPath());
        boolean compressProvided = DatabaseUtil.stringChecker(textFileHandler.getCompress());
        boolean encodingProvided = DatabaseUtil.stringChecker(textFileHandler.getEncoding());
        boolean nullFormatProvided = DatabaseUtil.stringChecker(textFileHandler.getNullFormat());

        JSONObject result = new JSONObject();
        JSONObject handler = new JSONObject();
        JSONObject parameter = new JSONObject();

        if (pathProvided) {
            parameter.put("path", textFileHandler.getPath());
            if (compressProvided) {
                parameter.put("compress", textFileHandler.getCompress());
            }
            if (encodingProvided) {
                encoding = textFileHandler.getEncoding();
            }
            if (nullFormatProvided) {
                nullFormat = textFileHandler.getNullFormat();
            }
            parameter.put("encoding", encoding);
            parameter.put("nullFormat", nullFormat);

            handler.put("name", isReading ? "txtfilereader": "txtfilewriter");
            handler.put("parameter", parameter);
            result.put(isReading ? "reader" : "writer", handler);

            return result;
        } else {
            throw new NoloseException(ResultCode.ERROR, "pathName参数缺失");
        }
    }
}
