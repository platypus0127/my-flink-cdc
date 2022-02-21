package com.hsb.utils;

public class KuduUtilsTest {
    public static void main(String[] args) throws ClassNotFoundException {
        String sql="{\"before\":null,\"after\":{\"PART_ID\":187953,\"CREATE_TIME\":1643270684,\"LAST_ACCESS_TIME\":0,\"PART_NAME\":\"p1=p0/p2=420\",\"SD_ID\":281967,\"TBL_ID\":94244},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1643270684000,\"snapshot\":\"false\",\"db\":\"metastore\",\"sequence\":null,\"table\":\"PARTITIONS\",\"server_id\":1,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":20350578,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1643270683623,\"transaction\":null}\n";
        String dbName = "metastore";
        String tbName = "partitions";
        KuduUtils kuduUtils = new KuduUtils();
        kuduUtils.open(String.format("%s_%s",dbName,tbName));
        kuduUtils.invoke(sql,"false");
    }
}
