package eu.europa.ec.digit.test.monitor.consumer.lag;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
public class KafkaUtils {

    @Value("${flashback.groupId:flashback-group}")
    protected String groupId;

    @Autowired
    protected KafkaProperties kafkaProperties;

    @Autowired
    protected KafkaTemplate kafkaTemplate;

    @Autowired
    protected ConsumerFactory consumerFactory;

    public Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId)
            throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(kafkaProperties.buildAdminProperties());
        try {
            ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = info.partitionsToOffsetAndMetadata().get();

            Map<TopicPartition, Long> groupOffset = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
                TopicPartition key = entry.getKey();
                OffsetAndMetadata metadata = entry.getValue();
                groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
            }
            return groupOffset;
        } finally {
            adminClient.close();
        }
    }

    public Map<TopicPartition, Long> getProducerOffsets(Map<TopicPartition, Long> consumerGrpOffset) {
        Consumer kafkaConsumer = consumerFactory.createConsumer();
        try {
            List<TopicPartition> topicPartitions = new LinkedList<>();
            for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffset.entrySet()) {
                TopicPartition key = entry.getKey();
                topicPartitions.add(new TopicPartition(key.topic(), key.partition()));
            }
            return kafkaConsumer.endOffsets(topicPartitions);
        } finally {
            kafkaConsumer.close();
        }
    }

    public Map<TopicPartition, Long> computeLags(
            Map<TopicPartition, Long> consumerGrpOffsets,
            Map<TopicPartition, Long> producerOffsets) {
        Map<TopicPartition, Long> lags = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
            Long producerOffset = producerOffsets.get(entry.getKey());
            Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
            long lag = Math.abs(producerOffset - consumerOffset);
            lags.putIfAbsent(entry.getKey(), lag);
        }
        return lags;
    }

    public void analyzeLag(String groupId) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> consumerGrpOffsets = getConsumerGrpOffsets(groupId);
        Map<TopicPartition, Long> producerOffsets = getProducerOffsets(consumerGrpOffsets);
        Map<TopicPartition, Long> lags = computeLags(consumerGrpOffsets, producerOffsets);
        for (Map.Entry<TopicPartition, Long> lagEntry : lags.entrySet()) {
            String topic = lagEntry.getKey().topic();
            int partition = lagEntry.getKey().partition();
            Long lag = lagEntry.getValue();
            log.info("Time={} | Lag for topic = {}, partition = {} is {}",
                    Instant.now(), topic, partition, lag);
        }
    }

    @Scheduled(fixedDelay = 5000L)
    public void liveLagAnalysis() throws ExecutionException, InterruptedException {
        analyzeLag(groupId);
    }

}
