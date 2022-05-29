package com.spring.kafka_test2.configuration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class ClipStreamConfiguration {

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        // 토픽과 멀티 토픽도 처리 가능
        KStream<String, String> stream = streamsBuilder.stream("clip5");
        // peek : 리턴 없이 일을 수행할 수 있음
//        stream.peek((key, value) -> System.out.println("Stream. Message=" + value))
//                .map((key, value) -> KeyValue.pair(key, "Hello, Listener(Peek의 Map 변환)")) // 메세지 형태 변환하고 싶을 때
//                .to("clip5-to"); // 어디로 발행 혹은 전달할 것이냐

        // groupby
//        stream.groupBy((key, value) -> value)
//                .count() // 같은 메세지별로 카운트, 다르다면 카운트 1
//                .toStream()
//                .peek((key, value) -> System.out.println("[GroupBy Stream] key=" + key + ", value=" + value));

        // branch : 복잡한 로직을 추가할 수 있음
        KStream<String, String>[] branches = stream.branch(
                (key, value) -> Long.valueOf(value) % 10 == 0, // 타임스탬프를 10으로 나눈 값
                (key, value) -> true // 나머지는 true
        );
        // 타임스탬프를 0으로 나눈 것이 맞을 경우 출력
        branches[0].peek((key, value) -> System.out.println("Branch 0. Message=" + value));
        // 아닐 경우 출력
        branches[1].peek((key, value) -> System.out.println("Branch 1. Message=" + value));

        return stream;
    }

    // 추가적 설정 필요
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfigs(){
        Map<String, Object> configs = new HashMap<>();
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "clip5-streams-id");
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // ==== Thread ====
        // consumer : thread를 여러 개 사용할 시 concurrency를 사용했음
        configs.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2"); // Thread 선언
        configs.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2); // 처리 방법 : 트랜잭션 or 최소 한 번 등
        return new KafkaStreamsConfiguration(configs);
    }

}
