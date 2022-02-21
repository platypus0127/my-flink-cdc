package com.hsb.utils;

import org.json.JSONObject;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class JsonTest {

    public static void main(String[] args) {

            String jsonStr = "{\"source\":{\"file\":\"mysql-bin.000003\",\"pos\":15350069,\"server_id\":1},\"position\":{\"transaction_id\":null,\"ts_sec\":1643191008,\"file\":\"mysql-bin.000003\",\"pos\":15350196,\"server_id\":1},\"databaseName\":\"test_db_1\",\"ddl\":\"ALTER  \\tTABLE\\tuser_1 ADD   xx4 varchar(255)\",\"tableChanges\":[{\"type\":\"ALTER\",\"id\":\"\\\"test_db_1\\\".\\\"user_1\\\"\",\"table\":{\"defaultCharsetName\":\"latin1\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"length\":11,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false},{\"name\":\"name\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"latin1\",\"length\":255,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false},{\"name\":\"address\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"latin1\",\"length\":1024,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false},{\"name\":\"phone_number\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"latin1\",\"length\":512,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false},{\"name\":\"email\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"latin1\",\"length\":255,\"position\":5,\"optional\":true,\"autoIncremented\":false,\"generated\":false},{\"name\":\"idx\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"latin1\",\"length\":255,\"position\":6,\"optional\":true,\"autoIncremented\":false,\"generated\":false},{\"name\":\"xx4\",\"jdbcType\":12,\"typeName\":\"VARCHAR\",\"typeExpression\":\"VARCHAR\",\"charsetName\":\"latin1\",\"length\":255,\"position\":7,\"optional\":true,\"autoIncremented\":false,\"generated\":false}]}}]}\n";
            JSONObject jsonObject = new JSONObject(jsonStr);
             System.out.println(jsonObject.get("ddl"));


    }



    @Test
    public void getFormatDate(){
        Date date = new Date();
        //long times = date.getTime();//时间戳
        long times = 164500427064L;
        System.out.println(times);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(date);
        System.out.println(dateString);
    }
}
