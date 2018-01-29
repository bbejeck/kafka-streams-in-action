package bbejeck.chapter_6.processor.cogrouping;


import bbejeck.model.ClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.processor.PunctuationType.STREAM_TIME;

/**
 * User: Bill Bejeck
 * Date: 8/12/17
 * Time: 10:54 AM
 */
public class CogroupingProcessor extends AbstractProcessor<String, Tuple<ClickEvent,StockTransaction>> {

    private KeyValueStore<String, Tuple<List<ClickEvent>,List<StockTransaction>>> tupleStore;
    public static final  String TUPLE_STORE_NAME = "tupleCoGroupStore";


    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        tupleStore = (KeyValueStore) context().getStateStore(TUPLE_STORE_NAME);
        CogroupingPunctuator punctuator = new CogroupingPunctuator(tupleStore, context());
        context().schedule(15000L, STREAM_TIME, punctuator);
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

}
