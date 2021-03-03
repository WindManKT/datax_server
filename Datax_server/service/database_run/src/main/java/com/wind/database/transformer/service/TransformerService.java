package com.wind.database.transformer.service;

import com.wind.entity.transformer.TransformerBuilder;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;

import java.util.List;

public interface TransformerService {
      JSONObject handleTransformer(List<JSONObject> jsonObjects);
}
