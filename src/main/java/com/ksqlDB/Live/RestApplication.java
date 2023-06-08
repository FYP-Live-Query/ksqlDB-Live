package com.ksqlDB.Live;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.security.auth.login.CredentialException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@RestController
public class RestApplication {
    public static String KSQLDB_SERVER_HOST = "10.8.100.246";
    public static int KSQLDB_SERVER_HOST_PORT = 8088;
    public AtomicInteger iterateID = new AtomicInteger(0);
    private final MeterRegistry meterRegistry;

    public RestApplication(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @GetMapping("/ksql")
    @CrossOrigin
    public void runQuery() {
        //		SpringApplication.run(LiveApplication.class, args);
        StringBuilder str1 = new StringBuilder("id-");
        long start=System.currentTimeMillis();
        str1.append(iterateID.incrementAndGet());
        String userId = str1.toString();
        ClientOptions options = ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_HOST_PORT);
        Client client = Client.create(options);

        // Send requests with the client by following the other examples

        client.streamQuery("SELECT ip,ROWTIME FROM network EMIT CHANGES;")
                .thenAccept(streamedQueryResult -> {
                    System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());

                    RowSubscriber subscriber = new RowSubscriber(userId, this.meterRegistry,start);
                    streamedQueryResult.subscribe(subscriber);
                }).exceptionally(e -> {
                    System.out.println("Request failed: " + e);
                    return null;
                });

        // Terminate any open connections and close the client
//		client.close();
    }
}
