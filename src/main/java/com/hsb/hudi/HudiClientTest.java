package com.hsb.hudi;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.examples.common.HoodieExampleDataGenerator;
import org.apache.hudi.index.HoodieIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*
* 参考：https://www.icode9.com/content-1-842259.html
* */
public class HudiClientTest {
    private static final Logger logger = LoggerFactory.getLogger(HudiClientTest.class);
    private static String tableType = HoodieTableType.COPY_ON_WRITE.name();


    public static void main(String[] args) throws Exception {

        //String tablePath = "hdfs://bitest01:8020/user/hive/hudi/huditable";
        String tablePath = "D:/工作/code/self_java/data";
        String tableName = "huditable";

        // 测试数据器
        HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("user","admin");
        // 初始化表
        Path path = new Path(tablePath);
        FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
        if (!fs.exists(path)) {
            // 检查路径是否存在
            // 初始化hudi table 创建hudi表的tablepath，写入初始化元数据信息
           // HoodieTableMetaClient.initTableType(hadoopConf, tablePath, HoodieTableType.COPY_ON_WRITE, tableName, HoodieAvroPayload.class.getName());
            HoodieTableMetaClient.withPropertyBuilder()
                    .setTableType(tableType)
                    .setTableName(tableName)
                    .setPayloadClassName(HoodieAvroPayload.class.getName())

                    .initTable(hadoopConf, tablePath);
        }

        // 创建write client conf
        HoodieWriteConfig hudiWriteConf = HoodieWriteConfig.newBuilder()
                // 数据schema
                .withSchema(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA)
                // 数据插入更新并行度
                .withParallelism(2, 2)
                // 数据删除并行度
                .withDeleteParallelism(2)
                // hudi表索引类型，内存
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                // 合并
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build())
                .withPath(tablePath)
                .forTable(tableName)
                .build();

        // 获得hudi write client
        HoodieJavaWriteClient<HoodieAvroPayload> client = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), hudiWriteConf);

        // 插入
        List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = insert(dataGen, client);
        // 更新
        upsert(dataGen, client, hoodieRecords);
        // 删除
        delete(dataGen, client, hoodieRecords);

        client.close();
    }

    /**
     * 删除
     *
     * @param dataGen       数据生成器
     * @param client        client
     * @param hoodieRecords records
     */
    public static void delete(HoodieExampleDataGenerator dataGen,
                              HoodieJavaWriteClient client,
                              List<HoodieRecord<HoodieAvroPayload>> hoodieRecords) {
        String newCommitTime = client.startCommit();
        logger.info("Starting Commit: " + newCommitTime);
        int deleteNum = hoodieRecords.size() / 2;
        List<HoodieKey> deleteRecords = hoodieRecords
                .stream()
                .map(HoodieRecord::getKey)
                .limit(deleteNum)
                .collect(Collectors.toList());
        List<WriteStatus> deleteStatus = client.delete(deleteRecords, newCommitTime);
        client.commit(newCommitTime, deleteStatus);
    }

    /**
     * 更新
     *
     * @param dataGen       数据生成器
     * @param client        client
     * @param hoodieRecords records
     * @return records
     */
    public static List<HoodieRecord<HoodieAvroPayload>> upsert(HoodieExampleDataGenerator dataGen,
                                                               HoodieJavaWriteClient client,
                                                               List<HoodieRecord<HoodieAvroPayload>> hoodieRecords) {
        String newCommitTime = client.startCommit();
        logger.info("Starting Commit: " + newCommitTime);
        List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateUpdates(newCommitTime, 4);
        hoodieRecords.addAll(toBeUpdated);
        List<HoodieRecord<HoodieAvroPayload>> writeRecords = hoodieRecords
                .stream()
                .map(record -> new HoodieRecord<HoodieAvroPayload>(record))
                .collect(Collectors.toList());
        List<WriteStatus> upsert = client.upsert(writeRecords, newCommitTime);
        client.commit(newCommitTime, upsert);
        return hoodieRecords;
    }

    /**
     * 插入
     *
     * @param dataGen 数据生成器
     * @param client  client
     */
    public static List<HoodieRecord<HoodieAvroPayload>> insert(HoodieExampleDataGenerator dataGen,
                                                               HoodieJavaWriteClient client) {
        // upsert
        // 开启提交
        String newCommitTime = client.startCommit();
        logger.info("Starting Commit: " + newCommitTime);

        // 生成数据
        List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, 10);
        List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = new ArrayList<>(records);
        List<HoodieRecord<HoodieAvroPayload>> writeRecords = hoodieRecords
                .stream()
                .map(record -> new HoodieRecord<HoodieAvroPayload>(record))
                .collect(Collectors.toList());
        // 获取upsertStatus
        List<WriteStatus> upsertStatus = client.upsert(writeRecords, newCommitTime);
        // 写入commit文件
        client.commit(newCommitTime, upsertStatus);

        return hoodieRecords;
    }
}
