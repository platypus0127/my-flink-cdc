package com.hsb.utils;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.cloudera.impala.jdbc.DataSource;
import com.hsb.flink_cdc_streaming.StreamLancher;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.sql.Types.FLOAT;

/*
 *
 * Kudu的demo，熟练impala操作kudu
 * */
public class KuduUtils {
    private static final Logger LOG = LoggerFactory.getLogger(StreamLancher.class);

    String key;
    String value;
    KuduClient kuduClient;
    String KuduMaster = "bitest01,bitest02,bitest03";
    String tableOpened; // kudu 表
    String tableOpenedImpala;  //impala 表
    KuduSession session;
    KuduTable table;
    Iterator<String> it;

    Delete delete;
    Upsert upsert;

    JSONObject dataJsonObject;
    String mysqlType;
    JSONObject mysqlJsonObject;
    Map<String, Object> mysqlObjectMap;
    AlterTableOptions alterTableOptions;
    String sql;
    String[] splitSql;
    String sqlType;
    String sqlTypeSplit;
    Type kuduType;
    String columnName;

    SimpleDateFormat  dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public static void main(String[] args) {
        new KuduUtils().open("metastore_partitions");
    }

    /*
     * 初始化连接
     * */
    public  void open(String tableName)   {

        kuduClient=new KuduClient.KuduClientBuilder(KuduMaster).build();
        tableOpenedImpala= tableName;
        tableOpened = "impala::test." + tableOpenedImpala;

        if(kuduClient != null) {
            try {
                table = kuduClient.openTable(tableOpened);
            } catch (KuduException e) {
                e.printStackTrace();
            }

        }


        session = kuduClient.newSession();
        //

        // 刷新模式：采取手动刷新
        SessionConfiguration.FlushMode flushMode = SessionConfiguration.FlushMode.MANUAL_FLUSH;
        session.setFlushMode(flushMode);

    }


    public void close() {
        if (session != null){
            try {
                session.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }

        if(kuduClient != null){
            try {
                kuduClient.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }

    }

    public void invoke(String  value,String is_init) throws ClassNotFoundException {
        Operation opertaion = null;

        //结构化
        JSONObject jsonObj = new JSONObject(value);
        LOG.warn("binlog" + value);
        if(value.contains("historyRecord")){ //DDL语句
            String historyRecordStr = jsonObj.getString("historyRecord");
            String  ddlSQL =  new JSONObject(historyRecordStr).getString("ddl").toLowerCase();
            if(ddlSQL.contains("alter") && ddlSQL.contains("table" )){ // 表级别的DDL
                if(ddlSQL.contains("alter") && ddlSQL.contains("table" ) && ddlSQL.contains("add" )){ //增加字段
                    List<SQLColumnDefinition> addColumns = DDLParser.getSqlADDColumnDefinitions(ddlSQL, "mysql");
                    int kuduAddedCount = 0 ;

                    for (SQLColumnDefinition  columnDefinition: addColumns) {
                        String columnName = columnDefinition.getColumnName().toLowerCase();
                        if(columnExists(columnName)){
                            continue;
                        }
                        SQLDataType dataType = columnDefinition.getDataType();
                        SQLExpr comment = columnDefinition.getComment();
                        LOG.error("DDL语句:"+ columnName + ":" + dataType + ":" + comment);
                        Type tranType = KuduTypeMapper.transDataType(dataType.toString());


                        try {
                            AlterTableResponse alterTableResponse = kuduClient.alterTable( this.tableOpened
                                    , new AlterTableOptions().addNullableColumn(columnName
                                    , KuduTypeMapper.transDataType(columnDefinition.getDataType().toString()))
                            );
                        } catch (KuduException e) {
                            e.printStackTrace();
                            LOG.error("DDL语句报错:"+ columnName + ":" + dataType + ":" + comment);
                            LOG.error("DDL语句报错:"+ e);
                            return;
                        }

                        if (kuduAddedCount >0){
                            StringBuffer sb = new StringBuffer();
                            sb.append(tableOpened).append("增加字段中");

                            refreshKudu(this.tableOpened, tableOpened);


                        }

                        LOG.warn("DDL表-增加字段:" + ddlSQL );
                    }



                }else{
                    LOG.warn("DDL表-非增加字段:" + ddlSQL);
                }

            }else{
                LOG.warn("DDL非表：" + historyRecordStr);
            }

        }else { //DML语句

                String op = jsonObj.getString("op");
                LOG.warn("op:" + op);
                Iterable<String> namedColumns;
                //c = create , u = update, d = delete , r = read
                if(op.equals("c") || op.equals("u") ) //插入数据
                {
                    LOG.warn("插入或者更新数据-" + op + ":"+ value);
                    JSONObject after = jsonObj.getJSONObject("after");
                    opertaion = table.newUpsert();
                    upsert(after, op, opertaion);

                }else if (op.equals("r") && is_init.equals("true")){ //初始化
                    LOG.warn("初始化数据" + op + ":"+ value);
                    JSONObject after = jsonObj.getJSONObject("after");
                    opertaion = table.newUpsert();
                    upsert(after, op, opertaion);
                }else if (op.equals("r") ){ //增量读取
                    LOG.warn("读取数据，不做任何处理：" + value);
                }else if (op.equals("d")){
                    LOG.warn("删除数据，不做任何处理：" + value);
                }
                else{
                    LOG.warn("不支持的类型" + value);
                }

        }




    }

    /**
     * refresh the table schema managed by impala
     * @param tableOpened the table handled currently
     * @param mysqlTable source table
     */
    public void refreshKudu(String tableOpened, String mysqlTable) throws ClassNotFoundException {
        String jdbcUrl = JobConfig.getProperty("impala.jdbc");
        Class.forName("com.cloudera.impala.jdbc.Driver");
        DataSource ds = new DataSource();
        ds.setURL(jdbcUrl);
        try(java.sql.Connection connection = ds.getConnection()) {

            String sqlInvalidate = "REFRESH " + tableOpened;

            PreparedStatement psInvalidate = connection.prepareStatement(sqlInvalidate);
            psInvalidate.execute();

            System.out.println(mysqlTable + " : " + "REFRESH");
            TimeUnit.SECONDS.sleep(60L);
            System.out.println(mysqlTable + " : " + "NAPED");

        } catch (SQLException | InterruptedException throwables) {
            throwables.printStackTrace();
        }
    }

    private void upsert(JSONObject after, String op, Operation opertaion) {
        Iterable<String> namedColumns;
        PartialRow partialRow = opertaion.getRow();
        namedColumns = new ArrayList<String>(after.keySet());
        LOG.warn("namedColumns:" + namedColumns .toString() );

        namedColumns.forEach( namedColumn ->{
            String formated = namedColumn.toLowerCase();
            ColumnSchema columnSchema = table.getSchema().getColumn(formated);
            Type kuduType = columnSchema.getType();
            String dataString = after.get(namedColumn).toString();

            //对时间戳进行转换
            if(formated.contains("time") && dataString.length()==13){
                dataString = dateFormat.format(Long.parseLong(dataString));
            }
            switch(kuduType){
                case STRING:
                    partialRow.addString(formated, dataString);
                    break;
                case INT32:
                    partialRow.addInt(formated,Integer.parseInt(dataString));
                    break;
                case INT64:
                    partialRow.addLong(formated,Long.parseLong(dataString));
                    break;
                case DOUBLE:
                    partialRow.addDouble(formated,Double.parseDouble(dataString));
                    break;
                case FLOAT:
                    partialRow.addDouble(formated,Float.parseFloat(dataString));
                    break;
                default:
                    LOG.error(table + "表，不支持的数据类型：" +  dataString );
                    throw new IllegalArgumentException("Illegal var type: " + kuduType);
            }

        } );

        try {

            Date date = new Date();
            partialRow.addString("data_update_time",dateFormat.format(date));
            session.apply(opertaion);
            session.flush();
        } catch (KuduException e) {
            throw new UnsupportedOperationException("Unspportted operation " + op + ": " +
                    this.table + " " + e.getMessage() + " " + e.getCause() + "\n" + after);
        }
    }



    /**
     * detect whether a column is existing in kudu table schema
     * @param columnName name of the column
     * @return bool to mark up
     */
    public boolean columnExists(String columnName) {
        try {
            this.table.getSchema().getColumnIndex(columnName);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }


}


