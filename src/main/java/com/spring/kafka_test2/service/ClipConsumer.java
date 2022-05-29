package com.spring.kafka_test2.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Service;

@Service
public class ClipConsumer extends AbstractConsumerSeekAware {

    private final Counter counter;

    public ClipConsumer(MeterRegistry meterRegistry) {
        this.counter = meterRegistry.counter("clip2-listener-counter", "group_id", "clip2-listener");
    }

    @KafkaListener(id = "clip1-listener-id", topics = "clip1-listener")
    public void listenClip1(String message){
        System.out.println("ClipConsumer -> listen");
        System.out.println("Message=" + message);
    }

    public void seek(){
        getSeekCallbacks().forEach((tp, consumerSeekCallback) -> consumerSeekCallback.seek(tp.topic(), tp.partition(), 0));
    }

    @KafkaListener(id = "clip5-to-listener", topics = "clip5-to")
    public void listenClip5(String message) {
        System.out.println("==== Clip5 Consumer ====");
        System.out.println("Clip5 Listener. Message=" + message);

    }
    
    @KafkaListener(id = "clip2-listener", topics = "clip2")
    public void listen(String message){
        System.out.println("Clip2Consumer -> listen");
        System.out.println("Message=" + message);
        counter.increment();
    }
}
