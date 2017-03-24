package bbejeck.chapter_5.timestamp_extractor;


import bbejeck.model.StockTransaction;
import bbejeck.util.datagen.DataGenerator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Date;

public class StockTransactionTimestampExtractor implements TimestampExtractor  {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        
        if(! (consumerRecord.value() instanceof StockTransaction)) {
            return System.currentTimeMillis();
        }

        StockTransaction stockTransaction = (StockTransaction) consumerRecord.value();
        Date transactionDate = stockTransaction.getTransactionTimestamp();
        return   (transactionDate != null) ? transactionDate.getTime() : consumerRecord.timestamp();
    }
}
