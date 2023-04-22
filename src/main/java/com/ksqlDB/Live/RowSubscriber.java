package com.ksqlDB.Live;

import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import io.micrometer.core.instrument.MeterRegistry;
public class RowSubscriber implements Subscriber<Row> {

    private Subscription subscription;
    private MeterRegistry meterRegistry;

    private final String userId;

    public RowSubscriber(String userId, MeterRegistry meterRegistry) {
        this.userId = userId;
        this.meterRegistry = meterRegistry;
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
        String datetime = row.getValue("EVENTTIMESTAMP").toString();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = null;
        try {
            date = sdf.parse(datetime);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        long updated = date.getTime();
//        System.out.println("Updated: "+ updated);
//        System.out.println("Current: "+ current);
        long latency = current - updated;
        meterRegistry.timer(userId).record(Duration.ofMillis(latency));
        System.out.println("latency: " + latency );

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

    public void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
}

