package bbejeck.chapter_6.punctuator;

import bbejeck.model.StockPerformance;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * User: Bill Bejeck
 * Date: 8/14/17
 * Time: 7:06 PM
 */
public class StockPerformancePunctuator implements Punctuator {


    private double differentialThreshold;
    private ProcessorContext context;
    private KeyValueStore<String, StockPerformance> keyValueStore;

    public StockPerformancePunctuator(double differentialThreshold,
                                      ProcessorContext context,
                                      KeyValueStore<String, StockPerformance> keyValueStore) {
        
        this.differentialThreshold = differentialThreshold;
        this.context = context;
        this.keyValueStore = keyValueStore;
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, StockPerformance> performanceIterator = keyValueStore.all();

        while (performanceIterator.hasNext()) {
            KeyValue<String, StockPerformance> keyValue = performanceIterator.next();
            String key = keyValue.key;
            StockPerformance stockPerformance = keyValue.value;

            if (stockPerformance != null) {
                if (stockPerformance.priceDifferential() >= differentialThreshold ||
                        stockPerformance.volumeDifferential() >= differentialThreshold) {
                    context.forward(key, stockPerformance);
                }
            }
        }
    }
}
