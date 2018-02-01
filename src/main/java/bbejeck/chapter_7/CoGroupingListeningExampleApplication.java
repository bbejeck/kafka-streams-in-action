package bbejeck.chapter_7;


import bbejeck.chapter_6.processor.KStreamPrinter;
import bbejeck.chapter_6.processor.cogrouping.ClickEventProcessor;
import bbejeck.chapter_6.processor.cogrouping.CogroupingProcessor;
import bbejeck.chapter_6.processor.cogrouping.StockTransactionProcessor;
import bbejeck.chapter_7.restore.LoggingStateRestoreListener;
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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static bbejeck.chapter_6.processor.cogrouping.CogroupingProcessor.TUPLE_STORE_NAME;

public class CoGroupingListeningExampleApplication {

    private static final Logger LOG = LoggerFactory.getLogger(CoGroupingListeningExampleApplication.class);

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


        Topology topology = new Topology();
        Map<String, String> changeLogConfigs = new HashMap<>();
        changeLogConfigs.put("retention.ms","120000" );
        changeLogConfigs.put("cleanup.policy", "compact,delete");

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(TUPLE_STORE_NAME);
        StoreBuilder<KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>>> builder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), eventPerformanceTuple);



        topology.addSource("Txn-Source", stringDeserializer, stockTransactionDeserializer, "stock-transactions")
                .addSource( "Events-Source", stringDeserializer, clickEventDeserializer, "events")
                .addProcessor("Txn-Processor", StockTransactionProcessor::new, "Txn-Source")
                .addProcessor("Events-Processor", ClickEventProcessor::new, "Events-Source")
                .addProcessor("CoGrouping-Processor", CogroupingProcessor::new, "Txn-Processor", "Events-Processor")
                .addStateStore(builder.withLoggingEnabled(changeLogConfigs), "CoGrouping-Processor")
                .addSink("Tuple-Sink", "cogrouped-results", stringSerializer, tupleSerializer, "CoGrouping-Processor");

        topology.addProcessor("Print", new KStreamPrinter("Co-Grouping"), "CoGrouping-Processor");


        MockDataProducer.produceStockTransactionsAndDayTradingClickEvents(50, 100, 100, StockTransaction::getSymbol);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);
        kafkaStreams.setGlobalStateRestoreListener(new LoggingStateRestoreListener());

        kafkaStreams.setUncaughtExceptionHandler((thread, exception) ->
            LOG.error("Thread [{}] encountered [{}]", thread.getName(), exception.getMessage())
        );

        kafkaStreams.setStateListener((newState, oldState) -> {
           if (oldState == KafkaStreams.State.REBALANCING && newState== KafkaStreams.State.RUNNING) {
               LOG.info("Topology Layout {}", topology.describe());
               LOG.info("Thread metadata {}", kafkaStreams.localThreadsMetadata());
           }
        });


        LOG.info("Co-Grouping App Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Thread.sleep(70000);
        LOG.info("Shutting down the Co-Grouping metrics App now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "cogrouping-restoring-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cogrouping-restoring-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cogrouping-restoring-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }


}
