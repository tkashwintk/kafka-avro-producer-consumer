package com.example.learnkafka.avroclients.service;

import com.example.learnkafka.Employee;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ProducerService {

    private final Logger log = LoggerFactory.getLogger(ProducerService.class.getName());

    // Inject Topic name
    @Value("${app.employee.topic}")
    private String employeeTopic;

    // Declare employeeProducer reference variable
    private final KafkaProducer<String, Employee> employeeProducer;

    // Inject employeeProducer into EmployeeService bean
    @Autowired
    public ProducerService(KafkaProducer<String, Employee> employeeProducer) {
        this.employeeProducer = employeeProducer;
    }

    public void produceData(Employee employee) {

        // Create a Producer Record
        ProducerRecord<String, Employee> producerRecord =
                new ProducerRecord<>(employeeTopic, employee.getId().toString(), employee);

        // Produce message to Kafka
        employeeProducer.send(producerRecord);

        log.info("Message produced successfully with key: {}", employee.getId());
    }

}
