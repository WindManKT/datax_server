package com.wind.database.textfile.controller;

import com.wind.database.textfile.service.TextFileService;
import com.wind.doamin.R;
import com.wind.entity.textfile.TextFileReader;
import com.wind.entity.textfile.TextFileWriter;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin
public class TextFileController {

    @Autowired
    TextFileService textFileService;

    @PostMapping("/readTextFile")
    public R readTextFile(@RequestBody TextFileReader textFileReader) {
        JSONObject result = textFileService.readTextFile(textFileReader);
        return (result == null) ? R.error().data("error", "参数缺失") : R.ok().data("json", result);
    }

    @PostMapping("/writeTextFile")
    public R writeTextFile(@RequestBody TextFileWriter textFileWriter) {
        JSONObject result = textFileService.writeTextFile(textFileWriter);
        return (result == null) ? R.error().data("error", "参数缺失") : R.ok().data("json", result);
    }
}
