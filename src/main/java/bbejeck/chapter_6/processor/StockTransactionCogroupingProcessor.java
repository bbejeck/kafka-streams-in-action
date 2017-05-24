package bbejeck.chapter_6.processor;


import bbejeck.model.StockTransaction;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;


public class StockTransactionCogroupingProcessor extends AbstractProcessor<String, StockTransaction> {

    private String storeName;
    private KeyValueStore<String, List<StockTransaction>> keyListStore;

    public StockTransactionCogroupingProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        keyListStore = (KeyValueStore) context().getStateStore(storeName);
    }

    @Override
    public void process(String key, StockTransaction value) {
        if (key != null) {
            List<StockTransaction> transactions = keyListStore.get(key);
            if (transactions == null) {
                transactions = new ArrayList<>();
            }

            transactions.add(value);
            keyListStore.put(key, transactions);
            //context().forward(key, value);
        }
    }

}
