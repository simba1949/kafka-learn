package vip.openpark.sdk.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import vip.openpark.sdk.interceptor.CodecProducerInterceptor;

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
        // 设置客户端id（可以使用 ProducerConfig.CLIENT_ID_CONFIG 里面常量）
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        // kafka 集群配置，多个以半角逗号分隔
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.8.43:9092");
        // 0：不保证消息的可靠性，1：保证消息的可靠性，-1：确保消息至少被一次的提交，all：确保消息被全部提交
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        // 拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CodecProducerInterceptor.class.getName());
        // 序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

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
        // 设置客户端id（可以使用 ConsumerConfig.CLIENT_ID_CONFIG 里面常量）
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        // 设置消费组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "hi-kafka-group");
        // kafka 集群配置，多个以半角逗号分隔
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.8.43:9092");
        // 反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaConsumer<>(properties);
    }
}