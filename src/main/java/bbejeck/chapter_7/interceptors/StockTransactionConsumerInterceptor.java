package bbejeck.chapter_7.interceptors;

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
 *
 * Using Object, Object as we'll get byte[] for the keys and values, hence we won't inspect the
 * messages.  If you want to inspect the messages you'll need to desarialize - inspect - serialize the
 * messages before returning.
 */
public class StockTransactionConsumerInterceptor implements ConsumerInterceptor<Object, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(StockTransactionConsumerInterceptor.class);

    public StockTransactionConsumerInterceptor() {
        LOG.info("Built StockTransactionConsumerInterceptor");
    }

    @Override
    public ConsumerRecords<Object, Object> onConsume(ConsumerRecords<Object, Object> consumerRecords) {
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

    private String buildMessage(Iterator<ConsumerRecord<Object, Object>> consumerRecords) {
        StringBuilder builder = new StringBuilder();
        while (consumerRecords.hasNext()) {
            builder.append(consumerRecords.next());
        }
        return builder.toString();
    }
}
