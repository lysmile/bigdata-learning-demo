package com.smile.demo.spark.example;

import com.smile.demo.spark.utils.MqUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author smile
 */
public class WordCountDemo {

    public static void main(String[] args) {
        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("wordCountDemo").setMaster("local[2]");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 设置日志打印级别
        jsc.sparkContext().setLogLevel("ERROR");
        // checkpoint目录
        jsc.checkpoint("/spark-streaming/word-count");

        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            jsc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(
                                    MqUtils.getTopics("wordCount"),
                                    MqUtils.buildKafkaParams("127.0.0.1:9092", "wordCountDemo"))
                    );

            JavaDStream<String> words = stream.flatMap((FlatMapFunction<ConsumerRecord<String, String>, String>) s -> {
                List<String> list = new ArrayList<>();
                list.add(s.value());
                return list.iterator();
            });

            // reduceByKey统计
            JavaPairDStream<String, Integer> countResult = words.mapToPair(
                    (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)).reduceByKey(
                            (Function2<Integer, Integer, Integer>) Integer::sum);
            countResult.print();


            //开窗函数 5秒计算一次 计算前15秒的数据聚合
            JavaPairDStream<String, Integer> reduceResult = countResult.reduceByKeyAndWindow(
                    (Function2<Integer, Integer, Integer>) Integer::sum,
                    Durations.seconds(15), Durations.seconds(5));
            reduceResult.print();

            jsc.start();
            jsc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}