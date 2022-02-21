package com.hsb.kudu;

import java.util.concurrent.ConcurrentHashMap;

public class BB_SelfSinkFactory {


    private  static final  ConcurrentHashMap<String, AA_SelfSink> sinkHashMap = new ConcurrentHashMap();


    public static AA_SelfSink getSink(String tableName,String is_init){
        AA_SelfSink bbSelfSink= sinkHashMap.get(tableName);
        if(bbSelfSink == null ){
            bbSelfSink=new AA_SelfSink(tableName,is_init);
            sinkHashMap.put(tableName,bbSelfSink);
            System.out.println("Applying changes to table: " + tableName);
        }
        return bbSelfSink;

    }

}
