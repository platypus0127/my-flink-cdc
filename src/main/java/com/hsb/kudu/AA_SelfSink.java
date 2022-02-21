package com.hsb.kudu;

import com.hsb.utils.KuduUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class AA_SelfSink extends RichSinkFunction<String> {

    String tableName;
    KuduUtils kuduUtils = null;
    String is_init;



    public AA_SelfSink(String  tableName, String is_init){
        this.tableName = tableName;
        this.is_init = is_init;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        kuduUtils = new KuduUtils();
        kuduUtils.open(tableName);
    }

    @Override
    public void invoke(String jsonStr, Context context) throws Exception {
        super.invoke(jsonStr, context);
        kuduUtils.invoke(jsonStr,is_init);
    }

    @Override
    public void close() throws Exception {
        super.close();
        kuduUtils.close();
    }



}
