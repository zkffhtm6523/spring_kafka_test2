package com.spring.kafka_test2.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Clip2Consumer {

    private final Counter counter;

    public Clip2Consumer(MeterRegistry meterRegistry) {
        this.counter = meterRegistry.counter("clip2-listener-counter", "group_id", "clip2-listener");
    }

    @KafkaListener(id = "clip2-listener", topics = "clip2")
    public void listen(String message){
        System.out.println("Clip2Consumer -> listen");
        System.out.println("Message=" + message);
        counter.increment();
    }
}
