package bbejeck.chapter_6.processor.cogrouping;


import bbejeck.model.ClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;


public class StockTransactionProcessor extends AbstractProcessor<String, StockTransaction> {


    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
    }

    @Override
    public void process(String key, StockTransaction value) {
        if (key != null) {
            Tuple<ClickEvent,StockTransaction> tuple = Tuple.of(null, value);
            context().forward(key, tuple);
        }
    }

}
