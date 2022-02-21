package com.hsb.utils;


import com.cloudera.impala.jdbc.DataSource;
import com.hsb.reader.MMetaColumn;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class JDBCUtilsTest {
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

    @Test
    public void preparedGetBeans() throws Exception {
//        String  DBName = "metastore";
//        String  TName = "PARTITIONS";

        String  DBName = "test_db_1".toLowerCase();
        String  TName = "user_1".toLowerCase();


        String jdbc="jdbc:mysql://bitest03:%s/%s?user=root&password=123456&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&net_write_timeout=200";
        String jdbcURL = String.format(jdbc, 3306,DBName);
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection(jdbcURL);
        List<MMetaColumn> mMetaColumns = JDBCUtils.preparedGetBeans(connection, MetaSQL, MMetaColumn.class, DBName, TName);


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

        System.out.println(String.format("drop table if exists test.%s_%s;",DBName,TName));

        String pfDefs = String.format("PRIMARY KEY (%s)", pkCloumn);
        String partDefs = String.format("\n) PARTITION BY HASH(%s) PARTITIONS 16\n", pkCloumn);
        String createSql = String.join(
                // 获取操作系统对应的换行符
                System.getProperty("line.separator"),
                String.format(" CREATE TABLE if not exists test.%s_%s (", DBName, TName),
                colDefs,
                " data_update_time String,",
                pfDefs,
                partDefs,
                System.getProperty("line.separator"),
                "STORED AS KUDU;");

        System.out.println( createSql);

        createSql(createSql,TName);

    }

    private void createSql(String createSql, String tableOpened) throws ClassNotFoundException {
        String impalaJDBC = JobConfig.getProperty("impala.jdbc");
        Class.forName("com.cloudera.impala.jdbc.Driver");
        Statement stmt = null;

        DataSource ds = new DataSource();
        ds.setURL(impalaJDBC);
        try(java.sql.Connection connection = ds.getConnection()) {

            stmt=connection.createStatement();
            stmt.execute(createSql);

            String sqlInvalidate = "REFRESH " + tableOpened;

            PreparedStatement psInvalidate = connection.prepareStatement(sqlInvalidate);
            psInvalidate.execute();
/*
            System.out.println(  " : " + "REFRESH");
            TimeUnit.SECONDS.sleep(60L);
            System.out.println(  " : " + "NAPED");*/

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        //TODO 建impala表
    }




}
