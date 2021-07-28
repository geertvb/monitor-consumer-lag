package eu.europa.ec.digit.test.monitor.consumer.lag;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MyConsumer {

    @KafkaListener(topics = "flashback")
    public void handleInternalCommand(ConsumerRecord consumerRecord) {
        try {
            log.info("Consuming: {}", consumerRecord);
        } catch (Throwable e) {
            log.error("Failed to consume: {}", consumerRecord, e);
        }
    }

}
