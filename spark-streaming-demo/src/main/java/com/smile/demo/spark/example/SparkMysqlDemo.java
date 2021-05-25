package com.smile.demo.spark.example;

import com.smile.demo.spark.entity.MqRequestData;
import com.smile.demo.spark.utils.JdbcConnectionPool;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Spark操作MYSQL 简易示例
 * @author smile
 */
public class SparkMysqlDemo {

    /**
     * 导出到MySQL
     */
    public static void exportToMysql(JavaDStream<MqRequestData> lines) {
        lines.foreachRDD(rdd -> rdd.foreachPartition(p -> {
            Connection conn = JdbcConnectionPool.getConnection();
            String sql = "INSERT INTO mysql_demo(id,name,created_time) VALUES(?,?,NOW())";
            PreparedStatement ps = conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            while (p.hasNext()) {
                MqRequestData data = p.next();
                ps.setString(1, String.valueOf(data.getValues().get("id")));
                ps.setString(2, String.valueOf(data.getValues().get("name")));
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
            ps.clearBatch();
            JdbcConnectionPool.close(conn, ps, null);
        }));
    }

}
