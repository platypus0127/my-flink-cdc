package com.hsb.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.hsb.flink_cdc_streaming.StreamLancher;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*
* sql 解析
* */
public class DDLParser {
    private static final Logger LOG = LoggerFactory.getLogger(StreamLancher.class);

    public static void main(String[] args) {
        String ddlSQL= "ALTER  \tTABLE\tuser_1 ADD   'xx4' varchar(255) comment \"这是注释\";\n";
        ddlSQL="alter table user_1 add column (c1 varchar(255),c2 varchar(255));";
        String dbType="mysql";
        List<SQLColumnDefinition> addColumnList = getSqlADDColumnDefinitions(ddlSQL, dbType);


        for (SQLColumnDefinition sqlColumnDefinition:addColumnList) {
            String columnName = sqlColumnDefinition.getColumnName().toLowerCase();
            SQLDataType dataType = sqlColumnDefinition.getDataType();
            SQLExpr comment = sqlColumnDefinition.getComment();

            System.out.println("DDL语句:"+ columnName + ":" + dataType + ":" + comment);
        }
    }

    @NotNull
    public static List<SQLColumnDefinition> getSqlADDColumnDefinitions(String ddlSQL, String dbType) {
        List<SQLStatement> sqlStatementList = SQLUtils.parseStatements(ddlSQL,dbType);
        SQLStatement sqlStatement = sqlStatementList.get(0);
        SQLAlterTableStatement alterSQL = (SQLAlterTableStatement) sqlStatement;
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        List<SQLColumnDefinition> addColumnList = new ArrayList<>();
        for (SQLAlterTableItem item : alterSQL.getItems()) {
            if (item instanceof MySqlAlterTableChangeColumn) {
                MySqlAlterTableChangeColumn renameColumn = (MySqlAlterTableChangeColumn) item;
                System.out.println(renameColumn.getAfterColumn());
            }
            if (item instanceof SQLAlterTableAddColumn) {
                SQLAlterTableAddColumn addColumn = (SQLAlterTableAddColumn) item;
                addColumnList.addAll(addColumn.getColumns());
            }
        }
        return addColumnList;
    }
}

