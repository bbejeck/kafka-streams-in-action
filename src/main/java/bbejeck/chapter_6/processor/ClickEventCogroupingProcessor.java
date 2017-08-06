package bbejeck.chapter_6.processor;


import bbejeck.model.DayTradingAppClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class ClickEventCogroupingProcessor extends AbstractProcessor<String, DayTradingAppClickEvent> {

    private KeyValueStore<String, List<StockTransaction>> transactionsStore;
    private KeyValueStore<String, List<DayTradingAppClickEvent>>  clickEventStore;
    private String transactionStoreName;
    private String clickEventStoreName;

    public ClickEventCogroupingProcessor(String transactionStoreName, String clickEventStoreName) {
        this.transactionStoreName = transactionStoreName;
        this.clickEventStoreName = clickEventStoreName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        transactionsStore = (KeyValueStore)context().getStateStore(transactionStoreName);
        clickEventStore = (KeyValueStore)context().getStateStore(clickEventStoreName);
        context().schedule(15000);
    }



    @Override
    public void process(String key, DayTradingAppClickEvent dayTradingAppClickEvent) {
        if (key != null) {
            List<DayTradingAppClickEvent> dayTradingAppClickEvents = clickEventStore.get(key);
            if (dayTradingAppClickEvents == null) {
                dayTradingAppClickEvents = new ArrayList<>();
            }

            dayTradingAppClickEvents.add(dayTradingAppClickEvent);
            clickEventStore.put(key, dayTradingAppClickEvents);
        }
    }

    @Override
    public void punctuate(long timestamp) {
      KeyValueIterator<String, List<DayTradingAppClickEvent>> clickEvents = clickEventStore.all();
      while(clickEvents.hasNext()){

          KeyValue<String,List<DayTradingAppClickEvent>> keyValue = clickEvents.next();
          String key = keyValue.key;

          List<DayTradingAppClickEvent> eventsSoFar = keyValue.value;
          List<StockTransaction> txnsSoFar = transactionsStore.delete(key);

          if (txnsSoFar == null) {
              txnsSoFar = new ArrayList<>();
          }

          if (eventsSoFar == null) {
               eventsSoFar = new ArrayList<>();
          }

          if (!eventsSoFar.isEmpty() || !txnsSoFar.isEmpty()) {
              Tuple<List<DayTradingAppClickEvent>, List<StockTransaction>> tuple = Tuple.of(eventsSoFar, txnsSoFar);
              context().forward(key, tuple);
          }

          clickEventStore.delete(key);
      }

    }
}
