package com.smile.demo.spark.example;

import com.redislabs.provider.redis.*;
import com.smile.demo.spark.entity.MqRequestData;
import com.smile.demo.spark.utils.MqUtils;
import org.apache.commons.lang3.StringUtils;
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
import redis.clients.jedis.Jedis;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Collections;
import java.util.List;

/**
 * Spark 操作redis
 * @author smile
 */
public class SparkRedisDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparkRedisDemo")
                .setMaster("yarn")
                .set("spark.redis.host", "127.0.0.1")
                .set("spark.redis.port", "6379")
                .set("spark.redis.auth", "123456")
                .set("spark.redis.db", "2");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        jsc.sparkContext().setLogLevel("ERROR");

        RedisConfig redisConfig = new RedisConfig(new RedisEndpoint(conf));
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(conf);
        RedisContext redisContext = new RedisContext(jsc.ssc().sc());

        try {
            JavaInputDStream<ConsumerRecord<String, String>> messages =
                    KafkaUtils.createDirectStream(
                            jsc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(
                                    MqUtils.getTopics("sparkRedisDemo"),
                                    MqUtils.buildKafkaParams("127.0.0.1:9092", "sparkRedis")));

            // map 将函数应用于每个RDD的每个元素，返回值是新的RDD
            JavaDStream<String> lines = messages.map(ConsumerRecord::value);
            // 设置缓存，内存不足时会刷到硬盘
            lines.persist(StorageLevel.MEMORY_AND_DISK());

            JavaPairDStream<String, Seq<String>> data2Redis = lines.mapToPair(d -> {
                MqRequestData data = MqUtils.parseMessage(d);
                String key = StringUtils.join(data.getProjectCode(), "", data.getDeviceCode());
                List<String> s = Collections.singletonList(d);
                // java List 转 scala Seq
                Seq<String> seq = JavaConverters.asScalaIteratorConverter(s.iterator()).asScala().toSeq();
                return new Tuple2<>(key, seq);
            });

            data2Redis.foreachRDD(rdd -> redisContext.toRedisLISTs(rdd.rdd(), 0, redisConfig, readWriteConfig));

            jsc.start();
            jsc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 另外一种更灵活获取Jedis连接的方法
     */
    private static Jedis getJedis(SparkConf conf) {
        return ConnectionPool.connect(new RedisEndpoint(conf));
    }


}