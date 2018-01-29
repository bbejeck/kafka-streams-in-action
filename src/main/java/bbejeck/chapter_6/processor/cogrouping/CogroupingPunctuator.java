package bbejeck.chapter_6.processor.cogrouping;


import bbejeck.model.ClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class CogroupingPunctuator implements Punctuator {

    private final KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> tupleStore;
    private final ProcessorContext context;

    public CogroupingPunctuator(KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> tupleStore, ProcessorContext context) {
        this.tupleStore = tupleStore;
        this.context = context;
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator = tupleStore.all();

        while (iterator.hasNext()) {
            KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> cogrouped = iterator.next();
            // if either list contains values forward results
            if (cogrouped.value != null && (!cogrouped.value._1.isEmpty() || !cogrouped.value._2.isEmpty())) {
                List<ClickEvent> clickEvents = new ArrayList<>(cogrouped.value._1);
                List<StockTransaction> stockTransactions = new ArrayList<>(cogrouped.value._2);

                context.forward(cogrouped.key, Tuple.of(clickEvents, stockTransactions));
                // empty out the current cogrouped results
                cogrouped.value._1.clear();
                cogrouped.value._2.clear();
                tupleStore.put(cogrouped.key, cogrouped.value);
            }
        }
        iterator.close();
    }
}
