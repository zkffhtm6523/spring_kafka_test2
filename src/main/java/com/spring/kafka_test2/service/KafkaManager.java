package com.spring.kafka_test2.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaManager {

    private final KafkaAdmin kafkaAdmin;
    private final AdminClient adminClient;

    public KafkaManager(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    public void describeTopicConfig() throws ExecutionException, InterruptedException {
        Collection<ConfigResource> resource = List.of(
                new ConfigResource(ConfigResource.Type.TOPIC, "clip4-listener")
        );
        DescribeConfigsResult result =  adminClient.describeConfigs(resource);
        System.out.println(result.all().get());
    }

    public void changeConfig() throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "clip4-listener");
        Map<ConfigResource, Collection<AlterConfigOp>> ops = new HashMap<>();
//        ops.put(resource, List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "6000"), AlterConfigOp.OpType.SET)));
        ops.put(resource, List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, null), AlterConfigOp.OpType.DELETE)));

        adminClient.incrementalAlterConfigs(ops);
        describeTopicConfig();
    }

    public void deleteRecords() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition("clip4-listener", 0);
        Map<TopicPartition, RecordsToDelete> target = new HashMap<>();
        target.put(tp, RecordsToDelete.beforeOffset(1)); // 보통 offset이 0번부터 시작하니가 0~1 삭제 예정

        DeleteRecordsResult deleteRecordsResult = adminClient.deleteRecords(target);
        Map<TopicPartition, KafkaFuture<DeletedRecords>> result = deleteRecordsResult.lowWatermarks();

        Set<Map.Entry<TopicPartition, KafkaFuture<DeletedRecords>>> entries = result.entrySet();
        for (Map.Entry<TopicPartition, KafkaFuture<DeletedRecords>> entry: entries) {
            System.out.println("Topic=" + entry.getKey().topic() + ", partition=" + entry.getKey().partition() + ", " + entry.getValue().get().lowWatermark());
        }
    }
    public void findAllConsumerGroups() throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult result = adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> groups =result.valid().get();

        for (ConsumerGroupListing group: groups) {
            System.out.println(group);
        }
    }

    public void deleteConsumerGroup() throws ExecutionException, InterruptedException {
        adminClient.deleteConsumerGroups(List.of("clip4-animal-listener")).all().get();
    }

    public void findAllOffsets() throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> target = new HashMap<>();
        target.put(new TopicPartition("clip4-listener", 0), OffsetSpec.latest());

        ListOffsetsResult result = adminClient.listOffsets(target);
        for (TopicPartition tp : target.keySet()) {
            System.out.println(
                      "Topic=" + tp.topic() +
                    ", Partition=" + tp.partition() +
                    ", Offsets=" + result.partitionResult(tp).get()
            );
        }
    }

}
