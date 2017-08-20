package bbejeck.chapter_7.restore;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Bill Bejeck
 * Date: 8/19/17
 * Time: 7:54 PM
 */
public class LoggingStateRestoreListener implements StateRestoreListener {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingStateRestoreListener.class);
    private final Map<TopicPartition, Long> totalToRestore = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> restoredSoFar = new ConcurrentHashMap<>();


    @Override
    public void onRestoreStart(TopicPartition topicPartition, String store, long start, long end) {
        long toRestore = end - start;
        totalToRestore.put(topicPartition, toRestore);
        LOG.info("Starting restoration for {} on topic-partition {} total to restore {}", store, topicPartition, toRestore);

    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String store, long start, long batchCompleted) {
        NumberFormat formatter = new DecimalFormat("#.##");

        long currentProgress = batchCompleted + restoredSoFar.getOrDefault(topicPartition, 0L);
        double percentComplete =  (double) currentProgress / totalToRestore.get(topicPartition);

        LOG.info("Completed {} for {}% of total restoration for {} on {}",
                batchCompleted, formatter.format(percentComplete * 100.00), store, topicPartition);
        restoredSoFar.put(topicPartition, currentProgress);
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String store, long totalRestored) {
        LOG.info("Restoration completed for {} on topic-partition {}", store, topicPartition);
        restoredSoFar.put(topicPartition, 0L);
    }
}
