package com.wind.database.transformer.service.impl;

import com.wind.base.exception.NoloseException;
import com.wind.database.transformer.service.TransformerService;
import com.wind.doamin.ResultCode;
import com.wind.entity.transformer.Substr;
import com.wind.entity.transformer.TransformerBuilder;
import com.wind.entity.transformer.UDF;
import net.sf.json.JSON;
import net.sf.json.JSONObject;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class TransformerServiceImpl implements TransformerService {
    public JSONObject handleTransformer(List<JSONObject> jsonObjects) {
        JSONObject transformer = new JSONObject();
        List<JSONObject> result = new ArrayList<>();

        if (jsonObjects.size() == 0) {
            throw new NoloseException(ResultCode.ERROR, "未找到有效transformer");
        }

        for (JSONObject jsonObject: jsonObjects) {
            JSONObject udfObject = new JSONObject();
            JSONObject parameter = new JSONObject();

            if (compareName(jsonObject,"Substr") || compareName(jsonObject,"Filter")) {
                if (compareName(jsonObject,"Substr")) {
                    udfObject.put("name", "dx_substr");
                }
                if (compareName(jsonObject,"Filter")) {
                    udfObject.put("name", "dx_filter");
                }

                indexTypeChecker(jsonObject.get("columnIndex"));
                int index = (int) jsonObject.get("columnIndex");
                indexChecker(index);
                parameter.put("columnIndex", index);

                List<Object> paras = (List<Object>) jsonObject.get("paras");
                checkParas(paras, 3);

                parameter.put("paras", paras);
                udfObject.put("parameter", parameter);

                result.add(udfObject);
            }
        }

        transformer.put("transformer", result);

        return transformer;
    }

    private boolean compareName(JSONObject jsonObject, String name) {
        return jsonObject.get("name").equals(name);
    }
    private void indexTypeChecker(Object columnIndex) {
        if (!(columnIndex instanceof Integer)) {
            throw new NoloseException(ResultCode.ERROR, "columnIndex应为整数类型");
        }
    }

    private void checkParas(List<Object> paras, int numOfArgs) {
        if (paras.size() > numOfArgs) {
            throw new NoloseException(ResultCode.ERROR, "paras中参数数量不得超过3个");
        }
        if (paras.size() < numOfArgs - 1) {
            throw new NoloseException(ResultCode.ERROR, "paras中第一二个参数必填");
        }
        if (!(paras.get(0) instanceof Integer)) {
            throw new NoloseException(ResultCode.ERROR, "paras中第一个参数需为整数类型");
        }
        for (int i = 1; i < paras.size(); i++) {
            if (!(paras.get(i) instanceof String)) {
                throw new NoloseException(ResultCode.ERROR, "paras中除第一个参数外其他参数需为字符串类型");
            }
        }
    }

    private void indexChecker(int index) {
        if (index < 1) {
            throw new NoloseException(ResultCode.ERROR, "columnIndex应大于等于1");
        }
    }
}
