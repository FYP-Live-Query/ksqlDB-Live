package com.ksqlDB.Live;

import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class RowSubscriber implements Subscriber<Row> {

    private Subscription subscription;

    public RowSubscriber() {
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
        long updated= (long) row.getValue("ROWTIME");
        long latency = current - updated;
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
}

