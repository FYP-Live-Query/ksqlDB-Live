package com.ksqlDB.Live;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.security.auth.login.CredentialException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@RestController
public class RestApplication {

    public static String KSQLDB_SERVER_HOST = "172.174.71.151";
    public static int KSQLDB_SERVER_HOST_PORT = 8088;
    public AtomicInteger iterateID = new AtomicInteger(0);
    private final List<Long> latencyValues = new CopyOnWriteArrayList<>();
    private final MeterRegistry meterRegistry;

    public RestApplication(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this::writeLatencyValuesToCsv, 1, 1, TimeUnit.MINUTES);
    }

    private synchronized void writeLatencyValuesToCsv() {
        try {
            // Calculate average and 90th percentile of latency values
            double averageLatency = latencyValues.stream()
                    .mapToLong(Long::longValue)
                    .average()
                    .orElse(Double.NaN);
            double percentile95Latency = latencyValues.stream()
                    .sorted()
                    .skip((long) (latencyValues.size() * 0.95))
                    .findFirst()
                    .orElse(0L);
            double percentile99Latency = latencyValues.stream()
                    .sorted()
                    .skip((long) (latencyValues.size() * 0.99))
                    .findFirst()
                    .orElse(0L);
            // Write average and 90th percentile of latency values to CSV file
            FileWriter csvWriter = new FileWriter("latency_values_ksql.csv", true);
            csvWriter.append(Double.toString(averageLatency));
            csvWriter.append(",");
            csvWriter.append(Double.toString(percentile95Latency));
            csvWriter.append(",");
            csvWriter.append(Double.toString(percentile99Latency));
            csvWriter.append("\n");
            csvWriter.flush();
            csvWriter.close();

            // Clear the latency values list
            latencyValues.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/ksql")
    @CrossOrigin
    public void runQuery() throws ExecutionException, InterruptedException {
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
        Thread streamingThread = new Thread(() -> {
            StreamedQueryResult streamedQueryResult = null;
            try {
                streamedQueryResult = client.streamQuery("SELECT ip,ROWTIME FROM network EMIT CHANGES;").get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            while (true) {
                // Block until a new row is available
                Row row = streamedQueryResult.poll();
                if (row != null) {
                    long current = System.currentTimeMillis();
                    long updated = (long) row.getValue("ROWTIME");
                    // skip processing rows that were created before the query started
                    if (updated > start) {
                        long latency = current - updated;
                        latencyValues.add(latency);
                        meterRegistry.timer(userId).record(Duration.ofMillis(latency));
                        System.out.println("latency: " + latency);
                    }
                    // Request the next row
                } else {
                    System.out.println("Query has ended.");
                }
            }
        }, "streaming Thread");
        streamingThread.start();
    }
}
