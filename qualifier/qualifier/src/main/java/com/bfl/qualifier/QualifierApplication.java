package com.bfl.qualifier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class QualifierApplication {

	public static void main(String[] args) {
		SpringApplication.run(QualifierApplication.class, args);
	}

	@Bean
	public RestTemplate restTemplate() {
		return new RestTemplate(); 
	}

}
