package bbejeck.chapter_9.restore;

import bbejeck.webserver.InteractiveQueryServer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

public class StateRestoreHttpReporter  implements StateRestoreListener {

    private final InteractiveQueryServer interactiveQueryServer;

    public StateRestoreHttpReporter(InteractiveQueryServer interactiveQueryServer) {
        this.interactiveQueryServer = interactiveQueryServer;
    }

    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {

    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {

    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {

    }
}
