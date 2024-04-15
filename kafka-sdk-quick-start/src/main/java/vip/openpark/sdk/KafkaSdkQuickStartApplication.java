package vip.openpark.sdk;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import vip.openpark.sdk.common.util.ProducerUtils;
import vip.openpark.sdk.config.KafkaConfig;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author anthony
 * @version 2024/4/15
 * @since 2024/4/15 13:16
 */
@Slf4j
public class KafkaSdkQuickStartApplication {
    public static void main(String[] args) throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(2);

        // 创建生产者
        KafkaProducer<String, String> kafkaProducer = KafkaConfig.producerConfig();

        final int msgCount = 10;
        new Thread(() -> {
            for (int i = 0; i < msgCount; i++) {
                try {
                    ProducerUtils.asyncSend(kafkaProducer, "hi-kafka", "async-send-key-" + i, "async-send-val");
                    ProducerUtils.syncSend(kafkaProducer, "hi-kafka", "sync-send-key-" + i, "sync-send-val");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            countDownLatch.countDown();
        }, "producer-thread").start();

        KafkaConsumer<String, String> kafkaConsumer = KafkaConfig.consumerConfig();
        // 订阅主题
        kafkaConsumer.subscribe(List.of("hi-kafka"));
        new Thread(() -> {
            kafkaConsumer.poll(Duration.ofSeconds(2000))
                .forEach(record -> {
                    // 消费消息
                    log.info("key={}, value={}", record.key(), record.value());
                });
            countDownLatch.countDown();
        }, "consumer-thread").start();


        countDownLatch.await();
    }
}