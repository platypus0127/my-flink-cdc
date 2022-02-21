package com.hsb.kudu;

import com.hsb.utils.KuduUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class KuduSink extends RichSinkFunction<String> {
    KuduUtils kuduUtils;
    String tableName;
    String is_init;

    public KuduSink(String tableName, String is_init){
        this.tableName=tableName;
        this.is_init = is_init;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.kuduUtils =new KuduUtils();
        kuduUtils.open(tableName);
        System.out.println("初始化连接："+ tableName);

    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println("invoke:" + value);
        kuduUtils.invoke(value,is_init);
    }

    @Override
    public void close() throws Exception {
        super.close();
        kuduUtils.close();
    }
}
