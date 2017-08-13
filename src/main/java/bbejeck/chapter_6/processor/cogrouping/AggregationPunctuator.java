package bbejeck.chapter_6.processor.cogrouping;


import bbejeck.model.ClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public class AggregationPunctuator implements Punctuator {

    private final KeyValueStore<String, Tuple<List<ClickEvent>,List<StockTransaction>>> tupleStore;
    private final ProcessorContext context;

    public AggregationPunctuator(KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> tupleStore, ProcessorContext context) {
        this.tupleStore = tupleStore;
        this.context = context;
    }

    @Override
    public void punctuate(long l) {
        KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator = tupleStore.all();

        while (iterator.hasNext()) {
            KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> cogrouping = iterator.next();

            if(cogrouping.value != null) {
                Tuple<List<ClickEvent>, List<StockTransaction>> tupleCopy =
                        Tuple.of(cogrouping.value._1, cogrouping.value._2);

                context.forward(cogrouping.key, tupleCopy);
                cogrouping.value._1.clear();
                cogrouping.value._2.clear();
                tupleStore.put(cogrouping.key, cogrouping.value);
            }
        }

    }
}
