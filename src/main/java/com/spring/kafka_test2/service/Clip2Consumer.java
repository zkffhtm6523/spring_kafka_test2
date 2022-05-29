package com.spring.kafka_test2.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Clip2Consumer {

    @KafkaListener(id = "clip2-listener", topics = "clip2")
    public void listen(String message){
        System.out.println("Clip2Consumer -> listen");
        System.out.println("Message=" + message);
    }
}
