package com.ksqlDB.Live;

import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.io.FileWriter;
public class RowSubscriber implements Subscriber<Row> {

    private Subscription subscription;
    private MeterRegistry meterRegistry;
    private final long start;
    private final List<Long> latencyValues = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private final String userId;

    public RowSubscriber(String userId, MeterRegistry meterRegistry,long start) {
        this.userId = userId;
        this.meterRegistry = meterRegistry;
        this.start =start;
        executorService.scheduleAtFixedRate(this::writeLatencyValuesToCsv, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        System.out.println("Subscriber is subscribed.");
        this.subscription = subscription;

        // Request the first row
        subscription.request(1);
    }

    @Override
    public synchronized void onNext(Row row) {
        long current = System.currentTimeMillis();
//        String datetime = row.getValue("EVENTTIMESTAMP").toString();
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//        Date date = null;
//        try {
//            date = sdf.parse(datetime);
//        } catch (ParseException e) {
//            throw new RuntimeException(e);
//        }
        long updated = (long) row.getValue("ROWTIME");
        // skip processing rows that were created before the query started
        if(updated > start){
            long latency = current - updated;
            latencyValues.add(latency);
            meterRegistry.timer(userId).record(Duration.ofMillis(latency));
            System.out.println("latency: " + latency );
        }
        // Request the next row
        subscription.request(1);
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
    @Override
    public synchronized void onError(Throwable t) {
        System.out.println("Received an error: " + t);
    }

    @Override
    public synchronized void onComplete() {
        System.out.println("Query has ended.");
    }

}

