package bbejeck.chapter_6.processor.cogrouping;


import bbejeck.model.ClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import io.dropwizard.cli.Cli;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class ClickEventProcessor extends AbstractProcessor<String, ClickEvent> {

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);

    }


    @Override
    public void process(String key, ClickEvent clickEvent) {
        if (key != null) {
            Tuple<ClickEvent, StockTransaction> tuple = Tuple.of(clickEvent, null);
            context().forward(key, tuple);
        }
    }

}
