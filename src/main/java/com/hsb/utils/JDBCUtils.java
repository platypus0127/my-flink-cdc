package com.hsb.utils;


import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JDBCUtils {
    public static <T> T res2Bean(Class<T> clazz, ResultSet rs) throws Exception {
        T t = clazz.newInstance();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        if (rs.next()) {
            for (int i = 0; i < columnCount; i++) {
                String columnLabel = rsmd.getColumnLabel(i + 1);
                Field field = clazz.getDeclaredField(columnLabel);
                field.setAccessible(true);
                if(field.getType() == Date.class) {
                    field.set(t, rs.getTimestamp(i + 1));
                } else {
                    field.set(t, rs.getObject(i + 1));
                }
            }
            return t;
        }
        return null;
    }

    public static <T> List<T> res2Beans(Class<T> clazz, ResultSet rs) throws Exception {
        List<T> results = new ArrayList<>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        while(rs.next()) {
            T t = clazz.newInstance();
            for (int i = 0; i < columnCount; i++) {
                Object columnVal = rs.getObject(i + 1);
                String columnLabel = rsmd.getColumnLabel(i + 1);
                Field field = clazz.getDeclaredField(columnLabel);
                field.setAccessible(true);
                field.set(t, columnVal);
            }
            results.add(t);
        }
        return results;
    }

    public static void preparedUpdate(Connection connection, String sql, Object... args) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(sql);
        for(int i = 0; i < args.length; i++) {
            ps.setObject(i+1, args[i]);
        }
        ps.execute();
        ps.close();
    }

    public static <T> List<T> preparedGetBeans(Connection conn, String sql, Class<T> clazz, Object... args) throws Exception {
        PreparedStatement ps = conn.prepareStatement(sql);

        for (int i = 0; i < args.length; i++) {
            ps.setObject(i+1, args[i]);
        }

        ResultSet rs = ps.executeQuery();

        return res2Beans(clazz, rs);
    }

    public static <T> T preparedGetBean(Connection conn, String sql, Class<T> clazz, Object... args) throws Exception {
        PreparedStatement ps = conn.prepareStatement(sql);

        for (int i = 0; i < args.length; i++) {
            ps.setObject(i+1, args[i]);
        }

        ResultSet rs = ps.executeQuery();

        return res2Bean(clazz, rs);
    }

    public static <T> List<T> staticGetBeans(Connection conn, String sql, Class<T> clazz) throws Exception {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql);

        return res2Beans(clazz, rs);
    }

    public static <T> T staticGetBean(Connection conn, String sql, Class<T> clazz) throws Exception {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql);

        return res2Bean(clazz, rs);
    }

}
