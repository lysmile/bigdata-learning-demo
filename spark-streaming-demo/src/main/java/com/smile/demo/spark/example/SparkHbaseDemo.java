package com.smile.demo.spark.example;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.smile.demo.spark.entity.MqRequestData;
import com.smile.demo.spark.utils.HbaseUtils;
import com.smile.demo.spark.utils.MqUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spark 操作hbase示例
 * @author smile
 */
public class SparkHbaseDemo {



    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sparkHbaseDemo").setMaster("yarn");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        jsc.sparkContext().setLogLevel("ERROR");

        try {
            JavaInputDStream<ConsumerRecord<String, String>> messages =
                    KafkaUtils.createDirectStream(
                            jsc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(MqUtils.getTopics("sparkHbaseDemo"),
                                    MqUtils.buildKafkaParams("127.0.0.1:9092", "sparkHbase")));

            JavaDStream<String> lines = messages.map(ConsumerRecord::value);
            // 设置缓存，内存不足时会刷到硬盘
            lines.persist(StorageLevel.MEMORY_AND_DISK());

            // 数据解析为 rowkey:dataValue
            JavaPairDStream<String, String> data2Hbase = lines.mapToPair(d -> {
                MqRequestData data = MqUtils.parseMessage(d);
                return new Tuple2<>(HbaseUtils.buildRowkey(data), JSON.toJSONString(data.getValues()));
            });

            // 方法1，spark内置的方法，可以直接使用RDD
            data2Hbase.foreachRDD(rdd -> HbaseUtils.saveBySpark(rdd, "spark_demo"));
            // 方法2，自己创建
            saveToHbaseByClient(lines);

            jsc.start();
            jsc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 自己创建连接的方式操作hbase
     */
    private static void saveToHbaseByClient(JavaDStream<String> lines) {
        lines.foreachRDD(rdd -> rdd.foreachPartition(p -> {
            Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create());
            Map<String, List<Put>> dataMap = new HashMap<>(8);
            while (p.hasNext()) {
                MqRequestData data = JSONObject.parseObject(p.next(), MqRequestData.class);
                String tableName = HbaseUtils.getTableName(data);
                Put put = HbaseUtils.buildPut(data, HbaseUtils.buildRowkey(data));
                List<Put> puts =  dataMap.getOrDefault(tableName, new ArrayList<>());
                puts.add(put);
                dataMap.put(tableName, puts);
            }

            dataMap.forEach((k,v) -> {
                try {
                    HbaseUtils.saveByClient(conn, k, v);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            conn.close();
        }));
    }


}