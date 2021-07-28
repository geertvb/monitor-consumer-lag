package eu.europa.ec.digit.test.monitor.consumer.lag;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableKafka
@EnableAsync
@EnableScheduling
@SpringBootApplication
public class MonitorConsumerLagApplication {

    public static void main(String[] args) {
        SpringApplication.run(MonitorConsumerLagApplication.class, args);
    }

}
