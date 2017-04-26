package bbejeck.chapter_6.processor;


import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;

public class StockPerformanceProcessor implements Processor<String, StockTransaction> {

    private ProcessorContext processorContext;
    private KeyValueStore<String, StockPerformance> keyValueStore;
    private String stateStoreName;
    private double differentialThreshold;

    public StockPerformanceProcessor(String stateStoreName, double differentialThreshold) {
        this.stateStoreName = stateStoreName;
        this.differentialThreshold = differentialThreshold;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
        keyValueStore = (KeyValueStore) this.processorContext.getStateStore(stateStoreName);
        this.processorContext.schedule(10000);
    }

    @Override
    public void process(String symbol, StockTransaction transaction) {
        StockPerformance stockPerformance = keyValueStore.get(symbol);

        if (stockPerformance == null) {
            stockPerformance = new StockPerformance();
        }

        stockPerformance.updatePriceStats(transaction.getSharePrice());
        stockPerformance.updateVolumeStats(transaction.getShares());
        stockPerformance.setLastUpdateSent(Instant.now());

        keyValueStore.put(symbol, stockPerformance);
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, StockPerformance> performanceIterator = keyValueStore.all();
        
        while (performanceIterator.hasNext()) {
            KeyValue<String, StockPerformance> keyValue = performanceIterator.next();
            String key = keyValue.key;
            StockPerformance stockPerformance = keyValue.value;
            if (stockPerformance.priceDifferential() >= differentialThreshold ||
                    stockPerformance.volumeDifferential() >= differentialThreshold) {
                this.processorContext.forward(key, stockPerformance);
            }
        }
    }

    @Override
    public void close() {
        keyValueStore.close();
    }
}
