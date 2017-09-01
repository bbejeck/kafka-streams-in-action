package bbejeck.chapter_7.interceptors;

import bbejeck.model.StockTransaction;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Bare bones implementation of a ConsumerInterceptor and simply prints results to the
 * stdout
 */
public class StockTransactionConsumerInterceptor implements ConsumerInterceptor<String, StockTransaction> {


    public StockTransactionConsumerInterceptor() {
        System.out.println("Built StockTransactionConsumerInterceptor");
    }

    @Override
    public ConsumerRecords<String, StockTransaction> onConsume(ConsumerRecords<String, StockTransaction> consumerRecords) {
        System.out.println("Intercepted records "+consumerRecords);
        return consumerRecords;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        
    }
}
