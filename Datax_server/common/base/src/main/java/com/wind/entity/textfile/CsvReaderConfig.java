package com.wind.entity.textfile;

import com.wind.utils.database.DatabaseUtil;
import lombok.Data;

@Data
public class CsvReaderConfig {
    private Boolean safetySwitch, skipEmptyRecords, useTextQualifier;

    public boolean csvReaderConfigChecker(CsvReaderConfig csvReaderConfig) {
        return DatabaseUtil.booleanChecker(csvReaderConfig.getSafetySwitch())
                && DatabaseUtil.booleanChecker(csvReaderConfig.getSkipEmptyRecords())
                && DatabaseUtil.booleanChecker(csvReaderConfig.getUseTextQualifier());
    }
}
