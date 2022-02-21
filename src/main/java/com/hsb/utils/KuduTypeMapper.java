package com.hsb.utils;

import org.apache.kudu.Type;

public class KuduTypeMapper {
    public static Type transDataType(String mysqlType) {
        switch (transDataTypeStr(mysqlType)) {
            case "BYTE"     :
            case "INT"      : return Type.INT32;
            case "LONG"     : return Type.INT64;
            case "DOUBLE"   : return Type.DOUBLE;
            default         : return Type.STRING;
        }
    }

    public static String transDatetime(String datetimeStr) {
        if (datetimeStr == null || datetimeStr.endsWith(".0")) {
            return datetimeStr;
        } else {
            if(datetimeStr.equalsIgnoreCase("null") || datetimeStr.equalsIgnoreCase("")) {
                return null;
            }
            return datetimeStr + ".0";
        }
    }

    public static String transDataTypeStr(String mysqlType) {
        String coreType = mysqlType.replaceAll("(?i)unsigned", "")
                .replaceAll("\\(\\d+\\)", "")
                .trim()
                .toUpperCase();
        switch (coreType) {
            case "DATETIME"     : return "DATETIME";
            case "DATE"         : return "DATE";
            case "YEAR"         :
            case "TIMESTAMP"    :
            case "TEXT"         :
            case "VARCHAR"      :
            case "CHAR"         : return "STRING";
            case "TINYINT"      :
            case "SMALLINT"     :
            case "INT"          :
            case "INTEGER"      :
            case "MEDIUMINT"    : return "INT";
            case "BIGINT"       : return "LONG";
            case "DECIMAL"      :
            case "FLOAT"        :
            case "DOUBLE"       : return "DOUBLE";
        }
        return coreType; // 默认
    }
}
