package bbejeck.chapter_6.processor;


import bbejeck.chapter_6.punctuator.StockPerformancePunctuator;
import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;

public class StockPerformanceProcessor extends AbstractProcessor<String, StockTransaction> {

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
        super.init(processorContext);
        keyValueStore = (KeyValueStore) context().getStateStore(stateStoreName);
        StockPerformancePunctuator punctuator = new StockPerformancePunctuator(differentialThreshold,
                                                                               context(),
                                                                               keyValueStore);
        
      context().schedule(10000, PunctuationType.WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(String symbol, StockTransaction transaction) {
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
    }
}
