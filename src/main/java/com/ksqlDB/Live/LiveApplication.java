package com.ksqlDB.Live;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;

@SpringBootApplication
public class LiveApplication {
	public static String KSQLDB_SERVER_HOST = "localhost";
	public static int KSQLDB_SERVER_HOST_PORT = 8088;

	public static void main(String[] args) {

//		SpringApplication.run(LiveApplication.class, args);
		ClientOptions options = ClientOptions.create()
				.setHost(KSQLDB_SERVER_HOST)
				.setPort(KSQLDB_SERVER_HOST_PORT);
		Client client = Client.create(options);

		// Send requests with the client by following the other examples

		client.streamQuery("SELECT * FROM riderLocations EMIT CHANGES;")
				.thenAccept(streamedQueryResult -> {
					System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());

					RowSubscriber subscriber = new RowSubscriber();
					streamedQueryResult.subscribe(subscriber);
				}).exceptionally(e -> {
					System.out.println("Request failed: " + e);
					return null;
				});


		// Terminate any open connections and close the client
//		client.close();
	}

}
