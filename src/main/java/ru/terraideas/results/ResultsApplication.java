package ru.terraideas.results;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class ResultsApplication {

	public static void main(String[] args) {
		SpringApplication.run(ResultsApplication.class, args);
	}
}
