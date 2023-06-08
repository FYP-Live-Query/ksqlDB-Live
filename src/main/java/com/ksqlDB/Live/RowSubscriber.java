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
    private final List<Long> latencyValues;


    private final String userId;

    public RowSubscriber(String userId, MeterRegistry meterRegistry,long start, List<Long> latencyValues) {
        this.userId = userId;
        this.meterRegistry = meterRegistry;
        this.start =start;
        this.latencyValues = latencyValues;

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

    @Override
    public synchronized void onError(Throwable t) {
        System.out.println("Received an error: " + t);
    }

    @Override
    public synchronized void onComplete() {
        System.out.println("Query has ended.");
    }

}

