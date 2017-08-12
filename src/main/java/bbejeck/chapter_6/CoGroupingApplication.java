package bbejeck.chapter_6;


import bbejeck.chapter_6.processor.ClickEventCogroupingProcessor;
import bbejeck.chapter_6.processor.CoGroupingAggregatingProcessor;
import bbejeck.chapter_6.processor.KStreamPrinter;
import bbejeck.chapter_6.processor.StockTransactionCogroupingProcessor;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.ClickEvent;
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

public class CoGroupingApplication {

    public static void main(String[] args) throws Exception {



        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        Serializer<String> stringSerializer = Serdes.String().serializer();
        Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> eventPerformanceTuple = StreamsSerdes.EventTransactionTupleSerde();
        Serializer<Tuple<List<ClickEvent>, List<StockTransaction>>> tupleSerializer = eventPerformanceTuple.serializer();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Deserializer<StockTransaction> stockTransactionDeserializer = stockTransactionSerde.deserializer();

        Serde<ClickEvent> clickEventSerde = StreamsSerdes.ClickEventSerde();
        Deserializer<ClickEvent> clickEventDeserializer = clickEventSerde.deserializer();
        Serde<List<StockTransaction>> txnListSerde = StreamsSerdes.TransactionsListSerde();
        Serde<List<ClickEvent>> eventListSerde = StreamsSerdes.EventListSerde();


        TopologyBuilder builder = new TopologyBuilder();


        builder.addSource("txn-source", stringDeserializer, stockTransactionDeserializer, "stock-transactions")
                .addSource( "events-source", stringDeserializer, clickEventDeserializer, "events")
                .addProcessor("txn-processor", StockTransactionCogroupingProcessor::new, "txn-source")
                .addProcessor("evnts-processor", ClickEventCogroupingProcessor::new, "events-source")
                .addProcessor("co-grouper", CoGroupingAggregatingProcessor::new, "txn-processor", "evnts-processor")
                .addStateStore(Stores.create(CoGroupingAggregatingProcessor.TUPLE_STORE_NAME)
                                     .withKeys(Serdes.String())
                                     .withValues(eventPerformanceTuple).persistent().build(), "co-grouper")
                .addSink("tuple-sink", "cogrouped-results", stringSerializer, tupleSerializer, "co-grouper");

        builder.addProcessor("print", new KStreamPrinter("Co-Grouping"), "co-grouper");


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
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }


}
