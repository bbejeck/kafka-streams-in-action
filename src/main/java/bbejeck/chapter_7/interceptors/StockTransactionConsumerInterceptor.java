package bbejeck.chapter_7.interceptors;

import bbejeck.model.StockTransaction;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;
import java.util.Map;

/**
 * Bare bones implementation of a ConsumerInterceptor and simply prints results to the
 * stdout
 */
public class StockTransactionConsumerInterceptor implements ConsumerInterceptor<String, StockTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(StockTransactionConsumerInterceptor.class);

    public StockTransactionConsumerInterceptor() {
        LOG.info("Built StockTransactionConsumerInterceptor");
    }

    @Override
    public ConsumerRecords<String, StockTransaction> onConsume(ConsumerRecords<String, StockTransaction> consumerRecords) {
        LOG.info("Intercepted ConsumerRecords {}",  buildMessage(consumerRecords.iterator()));
        return consumerRecords;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
         LOG.info("Commit information {}",  map);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        
    }

    private String buildMessage(Iterator<ConsumerRecord<String, StockTransaction>> consumerRecords) {
        StringBuilder builder = new StringBuilder();
        while (consumerRecords.hasNext()) {
            builder.append(consumerRecords.next());
        }
        return builder.toString();
    }
}
