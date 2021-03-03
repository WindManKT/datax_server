package com.wind.database.mysql.service.impl;

import com.wind.base.exception.NoloseException;
import com.wind.database.mysql.service.MySQLService;
import com.wind.doamin.ResultCode;
import com.wind.entity.mysql.MySQLReader;
import com.wind.entity.mysql.MySQLWriter;
import com.wind.utils.database.DatabaseUtil;
import net.sf.json.JSONObject;
import org.springframework.stereotype.Service;

@Service
public class MySQLServiceImpl extends MySQLServiceAbs implements MySQLService {

    @Override
    public JSONObject readSQL(MySQLReader mySQLReader) {

        boolean splitPKProvided = DatabaseUtil.stringChecker(mySQLReader.getSplitPK());
        boolean whereProvided = DatabaseUtil.listChecker(mySQLReader.getWhere());

        JSONObject result = sqlProcessor(mySQLReader, true);
        JSONObject reader = result.getJSONObject("reader");
        JSONObject parameter = reader.getJSONObject("parameter");

        if (splitPKProvided) {
            parameter.put("splitPk", mySQLReader.getSplitPK());
        }
        if (whereProvided) {
            parameter.put("where", mySQLReader.getWhere());
        }
        return result;
    }

    @Override
    public JSONObject writeSQL(MySQLWriter mySQLWriter) {

        int batchSize = 1024;
        String mode = "insert";
        int inputBatchSize = mySQLWriter.getBatchSize();
        boolean modeProvided = DatabaseUtil.stringChecker(mySQLWriter.getWriteMode());
        boolean modeTypeValid = false;
        if (modeProvided) {
            modeTypeValid = DatabaseUtil.modeCheckerSQL(mySQLWriter.getWriteMode());
        }
        boolean preSqlProvided = DatabaseUtil.listChecker(mySQLWriter.getPreSql());
        boolean postSqlProvided = DatabaseUtil.listChecker(mySQLWriter.getPostSql());
        boolean sessionProvided = DatabaseUtil.listChecker(mySQLWriter.getSession());

        if (DatabaseUtil.integerChecker(inputBatchSize)) {
            if (!DatabaseUtil.lessThanMax(inputBatchSize, 4096)) {
                throw new NoloseException(ResultCode.ERROR, "batchSize应小于等于4096");

            } else if (!DatabaseUtil.multipleOfTwo(inputBatchSize)) {
                throw new NoloseException(ResultCode.ERROR, "batchSize应为2的次方");
            } else {
                batchSize = inputBatchSize;
            }
        }
        if (modeProvided) {
            if (modeTypeValid) {
                mode = mySQLWriter.getWriteMode();
            } else {
                throw new NoloseException(ResultCode.ERROR, "writeMode参数错误");
            }
        }

        JSONObject result = sqlProcessor(mySQLWriter, false);
        JSONObject writer = result.getJSONObject("writer");
        JSONObject parameter = writer.getJSONObject("parameter");
        parameter.put("writeMode", mode);
        parameter.put("batchSize", batchSize);

        if (sessionProvided) {
            parameter.put("session", mySQLWriter.getSession());
        }
        if (preSqlProvided) {
            parameter.put("preSql", mySQLWriter.getPreSql());
        }
        if (postSqlProvided) {
            parameter.put("postSql", mySQLWriter.getPostSql());
        }
        return result;
    }
}
