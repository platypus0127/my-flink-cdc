package com.hsb.flink_cdc_batching;

import com.cloudera.impala.jdbc.DataSource;
import com.hsb.flink_cdc_streaming.StreamLancher;
import com.hsb.reader.MMetaColumn;
import com.hsb.utils.JDBCUtils;
import com.hsb.utils.JobConfig;

import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

/*
* 批量同步mysql数据到数据平台
* 功能：
*  1 ：表结构同步
*  2 : 批量数据的初始化
* */
public class BatchLancher {

    public static String MetaSQL = String.join(
            System.getProperty("line.separator"),
            "SELECT",
            "    COLUMN_NAME as name,",
            "    CASE",
            "        WHEN DATA_TYPE IN ('INTEGER', 'SMALLINT', 'INT', 'MEDIUMINT', 'TINYINT') THEN 'INT'",
            "        WHEN DATA_TYPE IN",
            "            ('VARCHAR', 'CHAR', 'LONGVARCHAR', 'NVARCHAR', 'NCHAR', 'LONGNVARCHAR', 'DATE', 'YEAR', 'TIME',",
            "             'TIMESTAMP', 'DATETIME', 'CLOB', 'LONGTEXT', 'TEXT', 'TINYTEXT', 'LONGBLOB', 'BLOG', 'TINYBLOB',",
            "             'MEDIUMBLOB', 'MEDIUMTEXT', 'SET', 'ENUM') THEN 'STRING'",
            "        WHEN DATA_TYPE IN ('NUMERIC','REAL') THEN 'BIGINT'",
            "        WHEN DATA_TYPE IN ('FLOAT') THEN 'FLOAT'",
            "        WHEN DATA_TYPE IN ('DECIMAL','DOUBLE') THEN 'DOUBLE'",
            "        WHEN DATA_TYPE IN ('BIT', 'BOOLEAN') THEN 'BOOLEAN'",
            "        WHEN DATA_TYPE IN ('BIGINT') THEN 'BIGINT'",
            "    END AS type,",
            "    COLUMN_KEY as iskey,",
            "    IF(COLUMN_COMMENT = NULL, COLUMN_NAME, COLUMN_COMMENT) AS comment",
            "FROM information_schema.COLUMNS t1",
            "    JOIN information_schema.TABLES t2 ON t1.TABLE_NAME = t2.TABLE_NAME AND t1.TABLE_SCHEMA = t2.TABLE_SCHEMA",
            "WHERE t2.TABLE_SCHEMA = ?",
            "    AND t2.TABLE_NAME = ?",
            "ORDER BY t1.ORDINAL_POSITION"
    );


    public static void main(String[] args) throws Exception {

        String myHostName = JobConfig.getProperty("mysql.hostName");
        String myUser = JobConfig.getProperty("mysql.user");
        String myPWD = JobConfig.getProperty("mysql.pwd");
        String myPort = JobConfig.getProperty("mysql.port");

        String DBName;
        String tbName;


        if("生产".equals("生产")){
            DBName="xy_detect".toLowerCase();
            tbName = "t_xy_yhb_order_txn".toLowerCase();
        }else {
            DBName="test_db_1".toLowerCase();
            tbName = "user_1".toLowerCase();
        }

        //表结构同步
        syncMetaData(myHostName,myPort,DBName.toLowerCase(),myUser,myPWD, tbName);

        //源表数据统计
        Integer lines = countLines(myHostName, myPort, DBName.toLowerCase(), myUser, myPWD, tbName);

        //同步数据
        String  is_init = "true";
        String[] databaseArrays = {DBName};
        String tableLists = String.format("%s.%s",DBName,tbName);

       // new StreamLancher().cdcStreaming(is_init, databaseArrays, tableLists,lines);

    }

    private static Integer countLines(String myHostName, String myPort, String DBName, String myUser, String myPWD, String tbName) throws SQLException, ClassNotFoundException {
        String jdbc= JobConfig.getProperty("mysql.jdbc");
        String jdbcURL = String.format(jdbc,myHostName, myPort,DBName,myUser,myPWD);
        Class<?> aClass = Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection(jdbcURL);
        String countSQL = String.format("select count(*) as `lines` from %s.%s ",DBName,tbName);
        PreparedStatement ps = connection.prepareStatement(countSQL);

        ResultSet rs = ps.executeQuery();
        Integer lines = 0 ;
        while(rs.next()){
            lines = rs.getInt("lines");

        }
        return lines;
    }

    /*
    * 获取元数据
    * */
    private static void syncMetaData(String myHostName, String port,String DBName,String myUser,String  myPWD, String tbName) throws Exception {

        String jdbc= JobConfig.getProperty("mysql.jdbc");
        String jdbcURL = String.format(jdbc,myHostName, port,DBName,myUser,myPWD);
        Class.forName("com.mysql.jdbc.Driver");
        System.out.println(jdbcURL);
        Connection connection = DriverManager.getConnection(jdbcURL);
        List<MMetaColumn> mMetaColumns = JDBCUtils.preparedGetBeans(connection, MetaSQL, MMetaColumn.class, DBName, tbName);


        //封装建表语句
        String colDefs = mMetaColumns.stream().map(
                mm -> String.format("    %s  %s  comment '%s' ,",
                        mm.getName().toLowerCase(),
                        mm.getType().toLowerCase(),
                        mm.getComment().toLowerCase())
        ).reduce("\n", String::concat);
        String pkCloumn = mMetaColumns.stream()
                .filter(mm -> mm.getIskey().equals("PRI"))
                .map(mm -> mm.getName().toLowerCase())
                .collect(Collectors.joining(","));

        System.out.println("pkCloumn主键"+ pkCloumn);
        String deleteTable = String.format("drop table if exists test.%s_%s;", DBName, tbName);


        String pfDefs = String.format("PRIMARY KEY (%s)", pkCloumn);
        String partDefs = String.format("\n) PARTITION BY HASH(%s) PARTITIONS 16\n", pkCloumn);
        String createSql = String.join(
                // 获取操作系统对应的换行符
                System.getProperty("line.separator"),
                String.format(" CREATE TABLE if not exists test.%s_%s (", DBName, tbName),
                colDefs,
                " data_update_time String,",
                pfDefs,
                partDefs,
                System.getProperty("line.separator"),
                "STORED AS KUDU;");

     createTB(deleteTable,String.format("test.%s_%s",DBName,tbName));
     createTB(createSql,String.format("test.%s_%s",DBName,tbName));






    }

    private static void createTB(String createSql,String tableOpened) throws ClassNotFoundException {
        String impalaJDBC = JobConfig.getProperty("impala.jdbc");
        Class.forName("com.cloudera.impala.jdbc.Driver");
        DataSource ds = new DataSource();
        Statement stmt = null;
        System.out.println("exexsql:" + createSql);
        ds.setURL(impalaJDBC);
        try(java.sql.Connection connection = ds.getConnection()) {

            stmt=connection.createStatement();
            stmt.execute(createSql);

        /*    String sqlInvalidate = "REFRESH " + tableOpened;

            PreparedStatement psInvalidate = connection.prepareStatement(sqlInvalidate);
            psInvalidate.execute();

            System.out.println(  " : " + "REFRESH");
            TimeUnit.SECONDS.sleep(60L);
            System.out.println(  " : " + "NAPED");*/

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }



}
