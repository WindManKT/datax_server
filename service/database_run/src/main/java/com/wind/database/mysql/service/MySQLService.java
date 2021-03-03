package com.wind.database.mysql.service;

import com.wind.entity.mysql.MySQLReader;
import com.wind.entity.mysql.MySQLWriter;
import net.sf.json.JSONObject;

public interface MySQLService {
    JSONObject readSQL(MySQLReader mySQLReader);

    JSONObject writeSQL(MySQLWriter mySQLWriter);
}
