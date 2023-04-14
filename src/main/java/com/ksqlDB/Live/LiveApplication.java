package com.ksqlDB.Live;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class LiveApplication {
	public static void main(String[] args) {
		SpringApplication.run(RestApplication.class, args);
	}

}
