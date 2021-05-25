package com.smile.demo.spark.utils;

import com.alibaba.fastjson.JSONObject;
import com.smile.demo.spark.constant.HbaseTableConsts;
import com.smile.demo.spark.entity.MqRequestData;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * hbase操作 工具类
 * @author smile
 */
public class HbaseUtils {

    /**
     * Spark内置的方式操作hbase
     */
    public static void saveBySpark(JavaPairRDD<String, String> rdd, String tableName) {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.mapred.outputtable", tableName);
        config.set("mapreduce.job.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = rdd.mapToPair(line -> {
            Put put = new Put(Bytes.toBytes(line._1));
            Map<String, Object> values = JSONObject.parseObject(line._2);
            values.forEach((k, v) -> {
                try {
                    put.addColumn(HbaseTableConsts.BYTES_COL_FAMILY, Bytes.toBytes(k), ObjectUtils.toByte(v));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });
        hbasePuts.saveAsNewAPIHadoopDataset(config);
    }

    /**
     * 自己建立client的方式操作Hbase
     * - 更灵活
     */
    public static void saveByClient(Connection conn, String tableName, List<Put> puts) throws IOException {
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            table.put(puts);
        }
    }

    public static String buildRowkey(MqRequestData data) {
        return StringUtils.join(data.getProjectCode(), HbaseTableConsts.ROWKEY_SEP, data.getDeviceCode(), HbaseTableConsts.ROWKEY_SEP, data.getCreated());
    }

    public static String getTableName(MqRequestData data) {
        return "demo";
    }


    public static Put buildPut(MqRequestData data, String rowkey) {
        Put put = new Put(Bytes.toBytes(rowkey));
        data.getValues().forEach((k, v) -> {
            try {
                put.addColumn(HbaseTableConsts.BYTES_COL_FAMILY, Bytes.toBytes(k), ObjectUtils.toByte(v));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return put;
    }




}
