package net.wessendorf;


import net.wessendorf.kafka.impl.KafkaDataSource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

public class SimpleKafkaRunner {


    final static ExecutorService executor = Executors.newFixedThreadPool(1);

    public static void main(String[] args) {

        final KafkaDataSource src = new KafkaDataSource("fooo");

        src.subscribe(new Flow.Subscriber<String>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(String item) {
                System.out.println("Kafka VAL: : " + item);
                subscription.request(1);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("It's over");
            }
        });

        executor.submit(src);
    }

}
