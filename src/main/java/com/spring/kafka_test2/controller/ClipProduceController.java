package com.spring.kafka_test2.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class ClipProduceController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ClipProduceController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/produce/clip2")
    public String produceClip2() throws InterruptedException {
        while(true){
            kafkaTemplate.send("clip2", String.valueOf(new Date().getTime()));
            Thread.sleep(2000L);
        }
    }

    @GetMapping("/produce/clip5")
    public String produceClip5() throws InterruptedException {
        while(true){
            kafkaTemplate.send("clip5", String.valueOf(new Date().getTime()));
            Thread.sleep(2000L);
        }
    }
}
