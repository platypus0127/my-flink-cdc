package com.hsb.hudi;

import org.apache.hudi.examples.java.HoodieJavaWriteClientExample;

public class HoodieExampleDataGeneratorTest {

    public static void main(String[] args) throws Exception {
/*
        HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();
        System.out.println(dataGen.toString());
        List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = dataGen.generateInserts("2021-12-31", 10);
        for ( HoodieRecord hoodieRecord: hoodieRecords) {
            System.out.println(hoodieRecord.toString());
        }
        System.out.println(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA);

*/


        String tablePath = "hdfs://bitest01:8020/user/hive/hudi/";
        //String tablePath = "D:/工作/code/self_java/data";
        String tableName = "hoodie_table";
        String[] arg = {tablePath,tableName};
        new HoodieJavaWriteClientExample().main(arg);
    }
}
