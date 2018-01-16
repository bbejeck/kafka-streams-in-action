package bbejeck.chapter_6.cancellation;


import bbejeck.chapter_6.punctuator.StockPerformancePunctuator;
import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

/**
 * Simple class demonstrating how to cancel a punctuation processing.  In this case
 * the punctuation is stopped after 15 minutes.
 *
 * This is a arbitrary example but demonstrates how we can cancel the punctuation.  After
 * cancelling, you could reschedule for a different time or schedule a different Punctuator
 * to run.
 */
public class StockPerformanceCancellingProcessor extends AbstractProcessor<String, StockTransaction> {

    private KeyValueStore<String, StockPerformance> keyValueStore;
    private String stateStoreName;
    private double differentialThreshold;
    private Cancellable cancellable;
    private Instant startInstant = Instant.now();
    private final long maxElapsedTimeForPunctuation = 15;


    public StockPerformanceCancellingProcessor(String stateStoreName, double differentialThreshold) {
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
        
        cancellable = context().schedule(10000, PunctuationType.WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(String symbol, StockTransaction transaction) {

        long elapsedTime = Duration.between(startInstant, Instant.now()).toMinutes();

        // cancels punctuation after 15 minutes
        if(elapsedTime >= maxElapsedTimeForPunctuation ) {
            cancellable.cancel();
        }

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
