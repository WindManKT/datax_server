package com.wind.database.textfile.service.impl;

import com.wind.base.exception.NoloseException;
import com.wind.database.textfile.service.TextFileService;
import com.wind.doamin.ResultCode;
import com.wind.entity.textfile.TextFileBlock;
import com.wind.entity.textfile.CsvReaderConfig;
import com.wind.entity.textfile.TextFileReader;
import com.wind.entity.textfile.TextFileWriter;
import com.wind.utils.database.DatabaseUtil;
import net.sf.json.JSONObject;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class TextFileServiceImpl extends TextFileServiceAbs implements TextFileService {
    public TextFileServiceImpl() {
    }

    @Override
    public JSONObject readTextFile(TextFileReader textFileReader) {
        TextFileBlock textFileBlock = new TextFileBlock();
        CsvReaderConfig csv = new CsvReaderConfig();

        boolean skipHeader = false;

        boolean columnProvided = textFileBlock.columnExists(textFileReader.getColumn());
        boolean columnValid = false;

        if (columnProvided) {
            columnValid = textFileBlock.columnValid(textFileReader.getColumn());
        }

        boolean fieldDelimiterProvided = DatabaseUtil.stringChecker(textFileReader.getFieldDelimiter());
        boolean skipHeaderProvided = DatabaseUtil.booleanChecker(textFileReader.getSkipHeader());
        boolean csvReaderProvided = csv.csvReaderConfigChecker(textFileReader.getCsvReaderConfig());

        JSONObject result = textFileProcessor(textFileReader, true);
        JSONObject reader = result.getJSONObject("reader");
        JSONObject parameter = reader.getJSONObject("parameter");

        List<JSONObject> blocksJson = new ArrayList<>();
        List<TextFileBlock> column = textFileReader.getColumn();

        if (fieldDelimiterProvided) {
            if (columnProvided) {
                for (TextFileBlock b : column) {
                    JSONObject blockJson = new JSONObject();
                    blockJson.put("index", b.getIndex());
                    blockJson.put("type", b.getType());
                    if (DatabaseUtil.stringChecker(b.getFormat())) {
                        blockJson.put("format", b.getFormat());
                    }
                    blocksJson.add(blockJson);
                }
            } else {
                throw new NoloseException(ResultCode.ERROR, "column参数缺失");
            }
            if (skipHeaderProvided) {
                skipHeader = textFileReader.getSkipHeader();
            }
            if (csvReaderProvided) {
                JSONObject csvReaderConfig = new JSONObject();
                csvReaderConfig.put("safetySwitch", textFileReader.gainSafetySwitch());
                csvReaderConfig.put("skipEmptyRecords", textFileReader.gainSkipEmptyRecords());
                csvReaderConfig.put("useTextQualifier", textFileReader.gainUseTextQualifier());
                parameter.put("csvReaderConfig", csvReaderConfig);
            }

            parameter.put("fieldDelimiter", textFileReader.getFieldDelimiter());
            parameter.put("skipHeader", skipHeader);
            parameter.put("column", blocksJson);

            return result;
        } else {
            throw new NoloseException(ResultCode.ERROR, "fieldDelimiter参数缺失");
        }
    }

    @Override
    public JSONObject writeTextFile(TextFileWriter textFileWriter) {
        String fieldDelimiter = ",";
        String fileFormat = "text";

        JSONObject result = textFileProcessor(textFileWriter, false );
        JSONObject reader = result.getJSONObject("writer");
        JSONObject parameter = reader.getJSONObject("parameter");

        boolean fileNameProvided = DatabaseUtil.stringChecker(textFileWriter.getFileName());
        boolean writeModeProvided = DatabaseUtil.stringChecker(textFileWriter.getWriteMode());
        boolean writeModeCorrect = false;
        if (writeModeProvided) {
             writeModeCorrect = DatabaseUtil.modeCheckerTxt(textFileWriter.getWriteMode());
        }
        boolean fieldDelimiterProvided = DatabaseUtil.stringChecker(textFileWriter.getFieldDelimiter());
        boolean dateFormatProvided = DatabaseUtil.stringChecker(textFileWriter.getDateFormat());
        boolean fileFormatProvided = DatabaseUtil.stringChecker(textFileWriter.getFileFormat());
        boolean headerProvided = DatabaseUtil.listChecker(textFileWriter.getHeader());

        if (!writeModeProvided) {
            throw new NoloseException(ResultCode.ERROR, "writeMode参数缺失");
        } else if (!writeModeCorrect) {
            throw new NoloseException(ResultCode.ERROR, "writeMode参数错误");
        } else if (fileNameProvided) {
            if (fieldDelimiterProvided) {
                fieldDelimiter = textFileWriter.getFieldDelimiter();
            }
            if (fileFormatProvided) {
                fileFormat = textFileWriter.getFileFormat();
            }
            if (dateFormatProvided) {
                parameter.put("dateFormat", textFileWriter.getDateFormat());
            }
            if (headerProvided) {
                parameter.put("header", textFileWriter.getHeader());
            }

            parameter.put("writeMode", textFileWriter.getWriteMode());
            parameter.put("fileName", textFileWriter.getFileName());
            parameter.put("fieldDelimiter", fieldDelimiter);
            parameter.put("fileFormat", fileFormat);
            return result;
        } else {
            throw new NoloseException(ResultCode.ERROR, "fileName参数缺失");
        }
    }
}
