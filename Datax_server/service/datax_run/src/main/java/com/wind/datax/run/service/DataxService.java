package com.wind.datax.run.service;

import net.sf.json.JSONObject;

public interface DataxService {
    void toJson(JSONObject jsonObject);

    JSONObject toSetting();
}
