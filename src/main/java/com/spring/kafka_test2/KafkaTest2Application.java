package com.spring.kafka_test2;

import com.spring.kafka_test2.service.ClipConsumer;
import com.spring.kafka_test2.service.KafkaManager;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Map;

@SpringBootApplication
public class KafkaTest2Application {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTest2Application.class, args);
    }

    @Bean
    public ApplicationRunner runner(KafkaManager kafkaManager,
                                    KafkaTemplate<String, String> kafkaTemplate,
                                    ClipConsumer clipConsumer,
                                    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry
    ){
        return args -> {
            // ==== 토픽 관리하기 ====

            // Topic 설정 조회
//            kafkaManager.describeTopicConfig();
//            // 설정 변경
//            kafkaManager.changeConfig(); // retention.ms의 value가 변경됨
//
//            // 기록 삭제
////            kafkaManager.deleteRecords();
//            kafkaManager.findAllConsumerGroups(); // 현재 존재하는 consumer 그룹 출력[1]
//            try {
//                kafkaManager.deleteConsumerGroup(); // group 출력 시 state가 empty가 아닌 stable이면 삭제되지 않음.
//            } catch (Exception e){
//                e.printStackTrace();
//            }
//            System.out.println("---- after delete consumer group ----");
//            kafkaManager.findAllConsumerGroups(); // 현재 존재하는 consumer 그룹 출력[2]
//
//            // 모든 offset 찾기
//            System.out.println("==== findAllOffsets ====");
//            kafkaManager.findAllOffsets();
//
//            // offset 탐색
////            kafkaTemplate.send("clip1-listener", "Hello, Listener"); // clip1 topic이 없으므로 우선 topic 생성과 message 생성
//            clipConsumer.seek();

            // ==== 모니터링 ====
            Map<MetricName, ? extends Metric> producerMetrics = kafkaTemplate.metrics();

            // kafkaListenerEndpointRegistry는 consumer의 kafkaListener 정보가 저장된다
            MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer("clip2-listener");

            // container start, stop 등 사용할 수 있고, metric 사용할 수 있어야 한다.
            Map<String, Map<MetricName, ? extends Metric>> consumerMetrics = container.metrics();
        };
    }
}
