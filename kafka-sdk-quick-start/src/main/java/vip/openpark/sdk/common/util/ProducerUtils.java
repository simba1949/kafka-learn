package vip.openpark.sdk.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

/**
 * @author anthony
 * @version 2024/4/15
 * @since 2024/4/15 14:02
 */
@Slf4j
public class ProducerUtils {
    /**
     * 异步发送消息
     *
     * @param kafkaProducer 生产者
     * @param topic         主体
     * @param key           键
     * @param value         值
     * @param <K>           键类型
     * @param <V>           值类型
     * @throws ExecutionException   异常
     * @throws InterruptedException 异常
     */
    public static <K, V> void asyncSend(KafkaProducer<K, V> kafkaProducer,
                                        String topic,
                                        K key,
                                        V value) throws ExecutionException, InterruptedException {
        send(kafkaProducer, topic, key, value, false);
    }

    /**
     * 同步发送消息
     *
     * @param kafkaProducer 生产者
     * @param topic         主体
     * @param key           键
     * @param value         值
     * @param <K>           键类型
     * @param <V>           值类型
     * @throws ExecutionException   异常
     * @throws InterruptedException 异常
     */
    public static <K, V> void syncSend(KafkaProducer<K, V> kafkaProducer,
                                       String topic,
                                       K key,
                                       V value) throws ExecutionException, InterruptedException {
        send(kafkaProducer, topic, key, value, true);
    }

    /**
     * @param kafkaProducer 生产者
     * @param topic         主体
     * @param key           键
     * @param value         值
     * @param isSync        是否同步，true为同步，false为异步
     * @param <K>           键类型
     * @param <V>           值类型
     * @throws ExecutionException   异常
     * @throws InterruptedException 异常
     */
    private static <K, V> void send(KafkaProducer<K, V> kafkaProducer,
                                    String topic, K key, V value,
                                    boolean isSync) throws ExecutionException, InterruptedException {
        final ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, value);
        if (isSync) {
            // 同步发送
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            log.info("send message success, offset={}", recordMetadata.offset());
        } else {
            // 异步发送
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        log.error("send message error, {}", e.getMessage());
                    } else {
                        log.info("send message success, offset is {}", recordMetadata.offset());
                    }
                }
            });
        }
    }
}