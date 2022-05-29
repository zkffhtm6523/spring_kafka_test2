package com.spring.kafka_test2.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Service;

@Service
public class ClipConsumer extends AbstractConsumerSeekAware {

    @KafkaListener(id = "clip1-listener-id", topics = "clip1-listener")
    public void listen(String message){
        System.out.println("ClipConsumer -> listen");
        System.out.println("Message=" + message);
    }

    public void seek(){
        getSeekCallbacks().forEach((tp, consumerSeekCallback) -> consumerSeekCallback.seek(tp.topic(), tp.partition(), 0));
    }
}
