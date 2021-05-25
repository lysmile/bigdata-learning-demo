package com.smile.demo.spark.utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 数据库连接池
 * @author smile
 */
public class JdbcConnectionPool {

    private static DataSource ds;

    static {
        try {
            Properties prop = new Properties();
            InputStream resourceAsStream = JdbcConnectionPool.class.getClassLoader().getResourceAsStream("jdbc.properties");
            prop.load(resourceAsStream);
            ds = DruidDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     */
    public static Connection getConnection() throws SQLException {
        return ds.getConnection();

    }

    /**
     * 关闭数据库的资源
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (null != ps) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}

