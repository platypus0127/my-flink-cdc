package com.hsb.flink_cdc_streaming;

import com.hsb.kudu.BB_SelfSinkFactory;
import com.hsb.kudu.KuduSink;
import com.hsb.utils.JobConfig;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.prometheus.client.Counter.build;


/*
     * 本例中演示使用flink-connect实时同步数据到kudu中
     * 文档参考：https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html
     * 参考：https://www.jianshu.com/p/02187f84f9a1
     *
     *
     * */
public class StreamLancher implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamLancher.class);


    public static void main(String[] args) throws Exception {
        LOG.warn(" 测试 cdc同步数据");

        String  is_init = "false";
        String[] databaseArrays = {"test_db_1", "metastore","pub_server_merchant_center_db","xy_detect"};
        String tableLists = "metastore.PARTITIONS" +
                ",test_db_1.user_1" +
                ",pub_server_merchant_center_db.t_merchant_shop_map" +
                ",pub_server_merchant_center_db.t_merchant_apply_base_info" +
                ",xy_detect.t_xy_yhb_order_txn"
                ;
        new StreamLancher().cdcStreaming(is_init, databaseArrays, tableLists,0);

    }

    public  void cdcStreaming( String is_init,  String[] databaseArrays, String tableLists,Integer lines) throws Exception {
        StartupOptions startupOptions = null ;



        Properties prop = new Properties();
        //1. 获取执行环境
        StreamExecutionEnvironment env=null;
        if("local".equals("local")){
            //本地模式
            Configuration conf = new Configuration();
            conf.setString(RestOptions.BIND_PORT,"8081-8083");
            env=StreamExecutionEnvironment.createLocalEnvironment(conf);

        }else { //非本地模式
            env= StreamExecutionEnvironment.getExecutionEnvironment();

        }

        if("true".equals(is_init))
        {
            startupOptions = StartupOptions.initial();// initial 初始化读取:历史数据+增量数据

        }else {
            startupOptions = StartupOptions.latest();


            //checkpoint
            String checkpointDir = JobConfig.getProperty("checkpoint.dir");

            // 1. 状态后端配置
            env.setStateBackend( new FsStateBackend(checkpointDir));


            // 2. 检查点配置
            env.enableCheckpointing(3*60*1000);

            // 高级选项
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setCheckpointTimeout(60000L);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
           // env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

            // 3. 重启策略配置
            // 固定延迟重启
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
            // 失败率重启
            env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));


//
//            //开启ck:每30s做一次checkpoint
//            env.enableCheckpointing(30* 1000);
//
//            // 设置statebackend，即checkpoint存储位置
//            //env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
//
//
//            // checkpoint 语义设置为 EXACTLY_ONCE，这是默认语义
//            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//            // 同一时间只能允许有一个 checkpoint
//            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//            // 最多允许 checkpoint 失败 3 次
//            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
//
//            // checkpoint 间隔
//            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(20*1000);
//
//            env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
//           //


        }


        String myHostName = JobConfig.getProperty("mysql.hostName");
        String myUser = JobConfig.getProperty("mysql.user");
        String myPWD = JobConfig.getProperty("mysql.pwd");
        String myPort = JobConfig.getProperty("mysql.port");


        //2. 通过FlinkCDC构建SourceFunction并读取数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(myHostName)
                .port(Integer.parseInt(myPort))
                .databaseList(databaseArrays)
                .tableList(tableLists) //如果不添加该参数,则消费指定数据库中所有表的数据.如果指定,指定方式为db.table
                .username(myUser)
                .password(myPWD)
                .deserializer(new JsonDebeziumDeserializationSchema()) //序列化器
                .includeSchemaChanges(true)// 获取DDL事件
                .startupOptions(startupOptions)
                .build();

        DataStreamSource<String> mysql_source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
        mysql_source.print();

        toDownDatabase(mysql_source,is_init);

        //初始化特有功能： 用于统计同步数据数量
        if("true".equals(is_init)){
           mysql_source.map(new RichMapFunction<String, String>() {

               //1:创建累加器
               private IntCounter numLines = new IntCounter();


               @Override
               public void open(Configuration parameters) throws Exception {
                   super.open(parameters);
                   //2:注册累加器
                   getRuntimeContext().addAccumulator("numLines",numLines);
               }

               @Override
               public String map(String s) throws Exception {
                   //3： 使用累加器
                   this.numLines.add(1);
                   Integer readLines = numLines.getLocalValue();
                   System.out.println(String.format("%s已读取的数据行数:",tableLists) + readLines+"-源数据:" + lines );
                   if(readLines.equals(lines)){
                       System.out.println("数据同步已完成，可以停止作业：" + readLines);
                       Thread.sleep(10*1000);
                       System.exit(0);
                   }
                   return s;
               }
           }).setParallelism(1)
           ;
        }




        //mysqlToKudu(mysql_source);
        env.execute("Print MySQL Snapshot + Binlog");
    }

    private  void toDownDatabase(DataStreamSource<String> mysql_source,String is_init ) {
        //mysqlToKudu(mysql_source);
        ArrayList<String> tableNameList = new ArrayList<>();
        //库名+"_”+表名
        tableNameList.add("test_db_1_user_1");
        tableNameList.add("metastore_partitions");
        tableNameList.add("pub_server_merchant_center_db_t_merchant_shop_map");
        tableNameList.add("pub_server_merchant_center_db_t_merchant_apply_base_info");
        tableNameList.add("xy_detect_t_xy_yhb_order_txn");

        mysql_source.print();
        //使用侧输出
        HashMap<String, OutputTag> outPutTagMap = new HashMap<>();

        for (String tableName:tableNameList) {
            OutputTag<String> outputTag = new OutputTag<String>(tableName){};
            outPutTagMap.put(tableName,outputTag);
        }

        SingleOutputStreamOperator<Object> outPutTagStream = mysql_source.process(new ProcessFunction<String, Object>() {
            @Override
            public void processElement(String jsonStr, Context context, Collector<Object> collector) throws Exception {
                String tableName = new JSONObject(jsonStr).getJSONObject("source").getString("table").toLowerCase();
                String DBName = new JSONObject(jsonStr).getJSONObject("source").getString("db").toLowerCase();
                String allTableName= DBName + "_" + tableName;
                if (outPutTagMap.containsKey(allTableName)) {
                    context.output(outPutTagMap.get(allTableName),jsonStr);
                } else {
                    System.out.println("不存在的表：" + allTableName + ":不需要同步");
                    collector.collect(jsonStr);
                }
            }
        });

        for (String allTableName: outPutTagMap.keySet()) {
            outPutTagStream.getSideOutput(outPutTagMap.get(allTableName)).addSink(BB_SelfSinkFactory.getSink(allTableName,is_init)).name(allTableName);

        }
    }

    //最普通的方式，不使用设计模式，没新建一个表都需要修改代码，非常不灵和
    private  void mysqlToKudu(DataStreamSource<String> mysql_source) {
        OutputTag<String> user_1_Out_Tag = new OutputTag<String>("user_1"){} ;
        OutputTag<String> user_2_Out_Tag = new OutputTag<String>("user_2"){} ;

        //使用侧输出 OutputTag
        SingleOutputStreamOperator<Object> outPutTag = mysql_source.process(
                new ProcessFunction<String, Object>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<Object> collector) throws Exception {
                        System.out.println("变更的sql:" + jsonStr);
                        JSONObject jsonObject = new JSONObject(jsonStr);
                        String tableName = jsonObject.getJSONObject("source").getString("table");
                        if (tableName.equals("user_1")){
                            System.out.println("user_1测输出:" + jsonStr);
                            context.output(user_1_Out_Tag,jsonStr);
                        } else if(tableName.equals("user_2")){
                            context.output(user_1_Out_Tag,jsonStr);
                        }else  {
                            System.out.println("此表不存在:"+ tableName);
                            collector.collect(jsonStr);
                        }

                    }
                }
        );

        // 输出数据：
        // 不使用设计模式
        outPutTag.getSideOutput(user_1_Out_Tag).addSink(new KuduSink("user_1","false")).name("user_1");
        outPutTag.getSideOutput(user_2_Out_Tag).addSink(new KuduSink("user_2","false")).name("user_2");
    }

}




