package com.cloudrangers.cloudpilotworker;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableRabbit
@SpringBootApplication
public class CloudpilotWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(CloudpilotWorkerApplication.class, args);
    }

}
