package net.wessendorf.flow;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;

public class MySubscriber<T> implements Subscriber<T> {
    private Subscription subscription;

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(T item) {
        System.out.println("Item : " + item);
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



    public static void main(String... args) throws Exception {

        SubmissionPublisher<String> publisher = new SubmissionPublisher<>();
        MySubscriber<String> subscriber = new MySubscriber<>();
        publisher.subscribe(subscriber);
        List<String> items = List.of("{foo: bar}", "<foo val=\"bar\" />");


        items.forEach(publisher::submit);


        while (true) {
            publisher.submit(UUID.randomUUID().toString());
            Thread.sleep(400);
        }

        // publisher.close();


    }







}    