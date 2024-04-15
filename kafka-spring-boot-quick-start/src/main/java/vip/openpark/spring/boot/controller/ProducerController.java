package vip.openpark.spring.boot.controller;

import jakarta.annotation.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author anthony
 * @version 2024/4/15
 * @since 2024/4/15 15:09
 */
@RestController
@RequestMapping(value = "producer")
public class ProducerController {
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping(value = "send")
    public String send(@RequestParam("key") String key,
                       @RequestParam("val") String val) {
        kafkaTemplate.send("hi-kafka", key, val);
        return "success";
    }
}