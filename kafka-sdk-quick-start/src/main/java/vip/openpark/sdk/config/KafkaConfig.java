package vip.openpark.sdk.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * @author anthony
 * @version 2024/4/15
 * @since 2024/4/15 13:26
 */
public class KafkaConfig {
    /**
     * 生产者配置
     *
     * @return KafkaProducer
     * @throws UnknownHostException 异常
     */
    public static <K, V> KafkaProducer<K, V> producerConfig() throws UnknownHostException {
        Properties properties = new Properties();
        // 设置客户端id
        properties.put("client.id", InetAddress.getLocalHost().getHostName());
        // kafka 集群配置，多个以半角逗号分隔
        properties.put("bootstrap.servers", "172.17.35.120:9092");
        // 0：不保证消息的可靠性，1：保证消息的可靠性，-1：确保消息至少被一次的提交，all：确保消息被全部提交
        properties.put("acks", "all");
        properties.put("retries", 0);
        // 序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(properties);
    }

    /**
     * 消费者配置
     *
     * @return KafkaConsumer
     * @throws UnknownHostException 异常
     */
    public static <K, V> KafkaConsumer<K, V> consumerConfig() throws UnknownHostException {
        Properties properties = new Properties();
        // 设置客户端id
        properties.put("client.id", InetAddress.getLocalHost().getHostName());
        // 设置消费组
        properties.put("group.id", "hi-kafka-group");
        // kafka 集群配置，多个以半角逗号分隔
        properties.put("bootstrap.servers", "172.17.35.120:9092");
        // 反序列化器
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(properties);
    }
}