package com.example.learnkafka.avroclients.service;

import com.example.learnkafka.Employee;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executors;

@Service
public class AppConsumer {

    private final Logger log = LoggerFactory.getLogger(AppConsumer.class.getName());
    private final KafkaConsumer<String, Employee> employeeConsumer;

    public AppConsumer(KafkaConsumer<String, Employee> employeeConsumer) {
        this.employeeConsumer = employeeConsumer;
    }

    @PostConstruct
    public void init() {
        Executors.newSingleThreadExecutor().execute(() -> {
            consumerAndProcessData();
        });
    }

    private void consumerAndProcessData() {
        employeeConsumer.subscribe(Arrays.asList("employee"));
        while(true) {
            ConsumerRecords<String, Employee> consumerRecords =
                    employeeConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Employee> record: consumerRecords) {
                log.info("Consuming Employee message");
                log.info("Key: {}", record.key());
                log.info("Value: {}", record.value());
                log.info("Partition: {}", record.partition());
                log.info("Offsset: {}", record.offset());
                // Write the message to DB
                // TBD
            }
            employeeConsumer.commitAsync();
        }
    }
}
