package com.example.backend;

import com.example.backend.config.LocalKafkaSupport;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BackendApplication {

	public static void main(String[] args) {
		LocalKafkaSupport.startIfNeeded();
		SpringApplication.run(BackendApplication.class, args);
	}

}
