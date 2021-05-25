package com.smile.demo.spark.utils;

import com.alibaba.fastjson.JSONObject;
import com.smile.demo.spark.entity.MqRequestData;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

/**
 * 消息队列 工具类
 * @author smile
 */
public class MqUtils {

    public static Map<String, Object> buildKafkaParams(String servers, String groupId) {
        // 构建kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>(8);
        //Kafka服务监听端口
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // earliest
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaParams.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        kafkaParams.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        kafkaParams.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";");
        return kafkaParams;
    }


    public static Collection<String> getTopics(String kafkaTopics) {
        Collection<String> topics = new HashSet<>();
        Collections.addAll(topics, kafkaTopics.split(","));
        return topics;
    }

    public static MqRequestData parseMessage(String message) {
        return JSONObject.parseObject(message, MqRequestData.class);
    }
}
