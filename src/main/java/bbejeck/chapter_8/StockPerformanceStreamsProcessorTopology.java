package bbejeck.chapter_8;

import bbejeck.chapter_6.transformer.StockPerformanceTransformer;
import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

/**
 * User: Bill Bejeck
 * Date: 9/10/17
 * Time: 3:54 PM
 */
public class StockPerformanceStreamsProcessorTopology {

    public static Topology build() {
        
        Serde<String> stringSerde = Serdes.String();
        Serde<StockPerformance> stockPerformanceSerde = StreamsSerdes.StockPerformanceSerde();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();


        StreamsBuilder builder = new StreamsBuilder();

        String stocksStateStore = "stock-performance-store";
        double differentialThreshold = 0.02;


        builder.addStateStore(Stores.create(stocksStateStore)
                .withStringKeys()
                .withValues(stockPerformanceSerde)
                .inMemory()
                .maxEntries(100)
                .build());

        builder.stream("stock-transactions", Consumed.with(stringSerde, stockTransactionSerde))
                .transform(() -> new StockPerformanceTransformer(stocksStateStore, differentialThreshold), stocksStateStore)
                .to("stock-performance", Produced.with(stringSerde, stockPerformanceSerde));

        return builder.build();
    }
}
