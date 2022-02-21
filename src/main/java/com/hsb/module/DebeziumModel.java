package com.hsb.module;

import lombok.Data;
import org.json.JSONObject;

@Data
public class DebeziumModel {
    private JSONObject before;
    private JSONObject after;
    //    private  String version;
    private  String connector;
    private  String name;
    private  long ts_ms;
    private  String db;
    private  String table;
    private  Integer server_id;
    private  String op;

}

