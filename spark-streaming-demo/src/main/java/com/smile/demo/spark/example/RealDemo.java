package com.smile.demo.spark.example;

import com.redislabs.provider.redis.ConnectionPool;
import com.redislabs.provider.redis.RedisEndpoint;
import com.smile.demo.spark.entity.MqRequestData;
import com.smile.demo.spark.utils.HbaseUtils;
import com.smile.demo.spark.utils.JdbcConnectionPool;
import com.smile.demo.spark.utils.MqUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 一个比较接近真实业务需求的示例，包含以下功能
 * - 接收kafka数据
 * - 解析数据为实体
 * - 存储到hbase
 * - 缓存到redis
 * - 导出到MySQL
 */
public class RealDemo {

    private static final String LOG_LEVEL = "ERROR";
    private static final String CHECK_POINT_DIR = "/spark-streaming/realDemo";
    private static final String KAFKA_TOPIC = "realDemo";
    private static final String KAFKA_GROUP_ID = "realDemo";
    private static final String KAFKA_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("realDemo")
                .setMaster("yarn")
                .set("spark.redis.host", "127.0.0.1")
                .set("spark.redis.port", "6379")
                .set("spark.redis.auth", "123456")
                .set("spark.redis.db", "2");

        Function0<JavaStreamingContext> contextFunction0 = (Function0<JavaStreamingContext>) () -> {
            JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));
            JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                    context,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(
                            MqUtils.getTopics(KAFKA_TOPIC),
                            MqUtils.buildKafkaParams(KAFKA_SERVERS, KAFKA_GROUP_ID)));
            // step1:解析消息体
            JavaDStream<MqRequestData> lines = convertInputStream(messages, conf);
            // 将后续多次使用DStream缓存起来
            lines.persist(StorageLevel.MEMORY_AND_DISK());
            // step2:保存到hbase
            saveToHbaseByClient(lines);
            // step3:导出到mysql
            exportToMysql(lines, conf);
            context.checkpoint(CHECK_POINT_DIR);
            return context;
        };

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(CHECK_POINT_DIR, contextFunction0);
        jsc.sparkContext().setLogLevel(LOG_LEVEL);
        jsc.start();
        jsc.awaitTermination();
    }


    private static JavaDStream<MqRequestData> convertInputStream(JavaInputDStream<ConsumerRecord<String, String>> input, SparkConf conf) {
        return input.mapPartitions(m -> {
            List<MqRequestData> dataList;
            try (Jedis jedis = ConnectionPool.connect(new RedisEndpoint(conf))) {
                dataList = new ArrayList<>();
                while (m.hasNext()) {
                    MqRequestData data = buildNewDataAndCount(MqUtils.parseMessage(m.next().value()), jedis);
                    dataList.add(data);
                }
            }
            return dataList.iterator();
        });
    }

    /**
     * 组装新的数据体，并统计项目数据量
     */
    private static MqRequestData buildNewDataAndCount(MqRequestData data, Jedis jedis) {
        data.getValues().put("newField", "这是一个新增的字段");
        // jedis应用示例：统计项目数据量
        jedis.incr(data.getProjectCode());
        return data;
    }

    /**
     * hbase操作示例
     * - 根据每条数据的不同，输出到不同的表
     */
    private static void saveToHbaseByClient(JavaDStream<MqRequestData> lines) {
        lines.foreachRDD(rdd -> rdd.foreachPartition(p -> {
            try (Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create())) {
                Map<String, List<Put>> dataMap = new HashMap<>(8);
                while (p.hasNext()) {
                    MqRequestData data = p.next();
                    String tableName = HbaseUtils.getTableName(data);
                    List<Put> puts = dataMap.getOrDefault(tableName, new ArrayList<>());
                    puts.add(HbaseUtils.buildPut(data, HbaseUtils.buildRowkey(data)));
                    dataMap.put(tableName, puts);
                }

                dataMap.forEach((k, v) -> {
                    try {
                        HbaseUtils.saveByClient(conn, k, v);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }));
    }

    /**
     * 导出到MySQL
     */
    private static void exportToMysql(JavaDStream<MqRequestData> lines, SparkConf conf) {
        lines.foreachRDD(rdd -> rdd.foreachPartition(p -> {
            java.sql.Connection conn = JdbcConnectionPool.getConnection();
            String sql = "INSERT INTO spark_db_demo(id,name,created_time) VALUES(?,?,NOW())";
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
