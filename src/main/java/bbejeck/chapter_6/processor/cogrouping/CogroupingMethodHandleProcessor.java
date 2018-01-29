package bbejeck.chapter_6.processor.cogrouping;


import bbejeck.model.ClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.processor.PunctuationType.STREAM_TIME;

/**
 * This class provides the same functionality as the
 * {@link CogroupingProcessor} but the {@link org.apache.kafka.streams.processor.Punctuator}
 * is provided via a method handle versus a concrete instance.
 */
public class CogroupingMethodHandleProcessor extends AbstractProcessor<String, Tuple<ClickEvent,StockTransaction>> {

    private KeyValueStore<String, Tuple<List<ClickEvent>,List<StockTransaction>>> tupleStore;
    public static final  String TUPLE_STORE_NAME = "tupleCoGroupStore";


    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        tupleStore = (KeyValueStore) context().getStateStore(TUPLE_STORE_NAME);
        context().schedule(15000L, STREAM_TIME, this::cogroup);
    }

    @Override
    public void process(String key, Tuple<ClickEvent, StockTransaction> value) {

        Tuple<List<ClickEvent>, List<StockTransaction>> cogroupedTuple = tupleStore.get(key);
        if (cogroupedTuple == null) {
             cogroupedTuple = Tuple.of(new ArrayList<>(), new ArrayList<>());
        }

        if(value._1 != null) {
            cogroupedTuple._1.add(value._1);
        }

        if(value._2 != null) {
            cogroupedTuple._2.add(value._2);
        }

        tupleStore.put(key, cogroupedTuple);
    }

    public void cogroup(long timestamp) {
        KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator = tupleStore.all();

        while (iterator.hasNext()) {
            KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> cogrouping = iterator.next();

            if (cogrouping.value != null && (!cogrouping.value._1.isEmpty() || !cogrouping.value._2.isEmpty())) {
                List<ClickEvent> clickEvents = new ArrayList<>(cogrouping.value._1);
                List<StockTransaction> stockTransactions = new ArrayList<>(cogrouping.value._2);

                context().forward(cogrouping.key, Tuple.of(clickEvents, stockTransactions));
                cogrouping.value._1.clear();
                cogrouping.value._2.clear();
                tupleStore.put(cogrouping.key, cogrouping.value);
            }
        }
        iterator.close();
    }

}
