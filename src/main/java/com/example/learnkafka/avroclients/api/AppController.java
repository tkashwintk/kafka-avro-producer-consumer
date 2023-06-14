package com.example.learnkafka.avroclients.api;


import com.example.learnkafka.Employee;
import com.example.learnkafka.avroclients.model.EmployeeDto;
import com.example.learnkafka.avroclients.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AppController {

    private final ProducerService producerService;

    @Autowired
    public AppController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/employee")
    public void addEmployee(@RequestBody EmployeeDto emp) {
        Employee employeeAvro = Employee
                .newBuilder()
                .setId(emp.getId())
                .setName(emp.getName())
                .build();
        producerService.produceData(employeeAvro);
    }
}
