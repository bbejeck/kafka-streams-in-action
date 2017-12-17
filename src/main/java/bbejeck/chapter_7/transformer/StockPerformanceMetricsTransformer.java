package bbejeck.chapter_7.transformer;


import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class StockPerformanceMetricsTransformer implements Transformer<String, StockTransaction, KeyValue<String, StockPerformance>> {

    private String stocksStateStore = "stock-performance-store";
    private KeyValueStore<String, StockPerformance> keyValueStore;
    private double differentialThreshold = 0.05;
    private ProcessorContext processorContext;
    private Sensor metricsSensor;
    private static AtomicInteger count = new AtomicInteger(1);


    public StockPerformanceMetricsTransformer(String stocksStateStore, double differentialThreshold) {
        this.stocksStateStore = stocksStateStore;
        this.differentialThreshold = differentialThreshold;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext processorContext) {
        keyValueStore = (KeyValueStore) processorContext.getStateStore(stocksStateStore);
        this.processorContext = processorContext;
        
        this.processorContext.schedule(5000, PunctuationType.WALL_CLOCK_TIME, this::doPunctuate);


        final String tagKey = "task-id";
        final String tagValue = processorContext.taskId().toString();
        final String nodeName = "StockPerformanceProcessor_"+count.getAndIncrement();
       metricsSensor = processorContext.metrics().addLatencyAndThroughputSensor("transformer-node",
                nodeName, "stock-performance-calculation",
                Sensor.RecordingLevel.DEBUG,
                tagKey,
                tagValue);
    }

    @Override
    public KeyValue<String, StockPerformance> transform(String symbol, StockTransaction stockTransaction) {
        if (symbol != null) {
            StockPerformance stockPerformance = keyValueStore.get(symbol);

            if (stockPerformance == null) {
                stockPerformance = new StockPerformance();
            }

            long start = System.nanoTime();
            stockPerformance.updatePriceStats(stockTransaction.getSharePrice());
            stockPerformance.updateVolumeStats(stockTransaction.getShares());
            stockPerformance.setLastUpdateSent(Instant.now());
            long end = System.nanoTime();
            
            processorContext.metrics().recordLatency(metricsSensor, start, end);

            keyValueStore.put(symbol, stockPerformance);
        }
        return null;
    }

    private void doPunctuate(long timestamp) {
        KeyValueIterator<String, StockPerformance> performanceIterator = keyValueStore.all();

        while (performanceIterator.hasNext()) {
            KeyValue<String, StockPerformance> keyValue = performanceIterator.next();
            String key = keyValue.key;
            StockPerformance stockPerformance = keyValue.value;

            if (stockPerformance != null) {
                if (stockPerformance.priceDifferential() >= differentialThreshold ||
                        stockPerformance.volumeDifferential() >= differentialThreshold) {
                    processorContext.forward(key, stockPerformance);
                }
            }
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public KeyValue<String, StockPerformance> punctuate(long l) {
        throw new UnsupportedOperationException("Should not call punctuate");
    }

    @Override
    public void close() {

    }
}
