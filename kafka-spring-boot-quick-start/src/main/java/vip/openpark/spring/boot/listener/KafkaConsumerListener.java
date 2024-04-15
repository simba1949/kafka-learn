package vip.openpark.spring.boot.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author anthony
 * @version 2024/4/15
 * @since 2024/4/15 15:13
 */
@Slf4j
@Component
public class KafkaConsumerListener {
    @KafkaListener(topics = {"hi-kafka"})
    public void onMessage(String message) {
        log.info("receive message: {}", message);
    }
}