package bbejeck.clients.consumer;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class TypedConsoleConsumer<K, V> {

    private Consumer<K, V> consumer;
    private ConsumerProperties consumerProperties;
    private volatile boolean keepConsuming = true;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public TypedConsoleConsumer(ConsumerProperties consumerProperties) {
        this.consumerProperties = consumerProperties;
    }


    public TypedConsoleConsumer<K, V> buildConsumer() {
        consumer = new KafkaConsumer<>(consumerProperties.getProperties());
        String[] topics = consumerProperties.getTopics().split(",");
        consumer.subscribe(Arrays.asList(topics));

        return this;
    }

    public void  start() {
        System.out.println("Consumer starting...");
        Runnable run = () -> {
            while (keepConsuming) {
                ConsumerRecords<K, V> records = consumer.poll(5000);
                for (ConsumerRecord<K, V> record : records) {
                    String message = String.format("TypedConsoleConsumer: key = %s value = %s with offset = %d partition = %d",
                            record.key(), record.value(), record.offset(), record.partition());
                    System.out.println(message);
                }
            }
            consumer.close();
        };
        executorService.submit(run);
    }


    public void stop() throws  Exception {
        System.out.println("Starting shutdown of console consumer");
        keepConsuming = false;
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        executorService.shutdownNow();
        System.out.println("Shut down now");
    }
}
