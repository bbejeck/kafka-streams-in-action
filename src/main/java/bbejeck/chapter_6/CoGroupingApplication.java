package bbejeck.chapter_6;


import bbejeck.chapter_6.processor.ClickEventCogroupingProcessor;
import bbejeck.chapter_6.processor.KStreamPrinter;
import bbejeck.chapter_6.processor.StockTransactionCogroupingProcessor;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.DayTradingAppClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;

import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset.LATEST;

public class CoGroupingApplication {

    public static void main(String[] args) throws Exception {



        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        Serializer<String> stringSerializer = Serdes.String().serializer();
        Serde<Tuple<List<DayTradingAppClickEvent>, List<StockTransaction>>> eventPerformanceTuple = StreamsSerdes.EventTransactionTupleSerde();
        Serializer<Tuple<List<DayTradingAppClickEvent>, List<StockTransaction>>> tupleSerializer = eventPerformanceTuple.serializer();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Deserializer<StockTransaction> stockTransactionDeserializer = stockTransactionSerde.deserializer();

        Serde<DayTradingAppClickEvent> clickEventSerde = StreamsSerdes.ClickEventSerde();
        Deserializer<DayTradingAppClickEvent> clickEventDeserializer = clickEventSerde.deserializer();
        Serde<List<StockTransaction>> txnListSerde = StreamsSerdes.TransactionsListSerde();
        Serde<List<DayTradingAppClickEvent>> eventListSerde = StreamsSerdes.EventListSerde();


        TopologyBuilder builder = new TopologyBuilder();
        String stocksStateStore = "stock-transactions-store";
        String dayTradingEventClicksStore = "day-trading-clicks-store";

        StockTransactionCogroupingProcessor transactionProcessor = new StockTransactionCogroupingProcessor(stocksStateStore);
        ClickEventCogroupingProcessor eventProcessor = new ClickEventCogroupingProcessor(stocksStateStore, dayTradingEventClicksStore);

        builder.addSource(LATEST, "txn-source", stringDeserializer, stockTransactionDeserializer, "stock-transactions")
                .addSource(LATEST, "events-source", stringDeserializer, clickEventDeserializer, "events")
                .addProcessor("txn-processor", () -> transactionProcessor, "txn-source")
                .addProcessor("evnts-processor", () -> eventProcessor, "events-source")
                .addStateStore(Stores.create(stocksStateStore).withStringKeys()
                        .withValues(txnListSerde).inMemory().maxEntries(100).build(),  "evnts-processor")
                .addStateStore(Stores.create(dayTradingEventClicksStore).withStringKeys()
                        .withValues(eventListSerde).inMemory().maxEntries(100).build(),  "txn-processor", "evnts-processor")
                .addSink("tuple-sink", "cogrouped-results", stringSerializer, tupleSerializer, "evnts-processor");

        builder.addProcessor("print", new KStreamPrinter("Co-Grouping"), "evnts-processor");


        MockDataProducer.produceStockTransactionsAndDayTradingClickEvents(50, 100, 100, StockTransaction::getSymbol);

        KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfig);
        System.out.println("Co-Grouping App Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(70000);
        System.out.println("Shutting down the Co-Grouping App now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "cogrouping-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cogrouping-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cogrouping-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }


}
