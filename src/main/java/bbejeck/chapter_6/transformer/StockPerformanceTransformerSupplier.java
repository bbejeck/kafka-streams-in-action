package bbejeck.chapter_6.transformer;


import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class StockPerformanceTransformerSupplier implements TransformerSupplier<String, StockTransaction, KeyValue<String, StockPerformance>> {

    private String stateStoreName;
    private double differentialThreshold;

    public StockPerformanceTransformerSupplier(String stateStoreName, double differentialThreshold) {
        this.stateStoreName = stateStoreName;
        this.differentialThreshold = differentialThreshold;
    }


    @Override
    public Transformer<String, StockTransaction, KeyValue<String, StockPerformance>> get() {
        return new StockPerformanceTransformer(stateStoreName, differentialThreshold);
    }

    private class StockPerformanceTransformer implements Transformer<String, StockTransaction, KeyValue<String, StockPerformance>> {

        private String stateStoreName;
        private double differentialThreshold;
        private ProcessorContext processorContext;
        private KeyValueStore<String, StockPerformance> keyValueStore;

        public StockPerformanceTransformer(String stateStoreName, double differentialThreshold) {
            this.stateStoreName = stateStoreName;
            this.differentialThreshold = differentialThreshold;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext processorContext) {
            this.processorContext = processorContext;
            keyValueStore = (KeyValueStore) this.processorContext.getStateStore(stateStoreName);
            this.processorContext.schedule(15000);
        }

        @Override
        public KeyValue<String, StockPerformance> transform(String symbol, StockTransaction transaction) {
            if (symbol != null) {
                StockPerformance stockPerformance = keyValueStore.get(symbol);

                if (stockPerformance == null) {
                    stockPerformance = new StockPerformance();
                }

                stockPerformance.updatePriceStats(transaction.getSharePrice());
                stockPerformance.updateVolumeStats(transaction.getShares());
                stockPerformance.setLastUpdateSent(Instant.now());

                keyValueStore.put(symbol, stockPerformance);
            }
            return null;
        }

        @Override
        public KeyValue<String, StockPerformance> punctuate(long timestamp) {

            KeyValueIterator<String, StockPerformance> performanceIterator = keyValueStore.all();
            while (performanceIterator.hasNext()) {
                KeyValue<String, StockPerformance> keyValue = performanceIterator.next();
                StockPerformance stockPerformance = keyValue.value;

                if (stockPerformance != null) {
                    if (stockPerformance.priceDifferential() >= differentialThreshold ||
                            stockPerformance.volumeDifferential() >= differentialThreshold) {
                             this.processorContext.forward(keyValue.key, stockPerformance);
                    }
                }
            }
            return null;
        }

        @Override
        public void close() {
            //no-op
        }

    }
}
