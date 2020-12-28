package bbejeck.chapter_6;

import bbejeck.chapter_6.processor.KStreamPrinter;
import bbejeck.chapter_6.processor.cogrouping.ClickEventProcessor;
import bbejeck.chapter_6.processor.cogrouping.CogroupingProcessor;
import bbejeck.chapter_6.processor.cogrouping.StockTransactionProcessor;
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

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static bbejeck.chapter_6.processor.cogrouping.CogroupingProcessor.TUPLE_STORE_NAME;

public class CoGroupingApplication {

  public static void main(String[] args) throws Exception {

    StreamsConfig streamsConfig = new StreamsConfig(getProperties());

    // Serializer and Deserializer for String
    Deserializer<String> stringDeserializer = Serdes.String().deserializer();
    Serializer<String> stringSerializer = Serdes.String().serializer();

    // Serde and Serializer for Tuple<List<ClickEvent>, List<StockTransaction>> Stream
    Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> eventPerformanceTuple =
        StreamsSerdes.EventTransactionTupleSerde();
    Serializer<Tuple<List<ClickEvent>, List<StockTransaction>>> tupleSerializer =
        eventPerformanceTuple.serializer();

    // Serde and Deserializier for StockTransaction stream
    Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
    Deserializer<StockTransaction> stockTransactionDeserializer =
        stockTransactionSerde.deserializer();

    // Serde and Deserializer for ClickEvent stream
    Serde<ClickEvent> clickEventSerde = StreamsSerdes.ClickEventSerde();
    Deserializer<ClickEvent> clickEventDeserializer = clickEventSerde.deserializer();

    // StoreBuilder to persist state for: Processor to update and Punctuator to punctuate
    Map<String, String> changeLogConfigs = Map.of(
            "retention.ms", "120000",
            "cleanup.policy", "compact,delete");
    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(TUPLE_STORE_NAME);
    StoreBuilder<KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>>>
        storeBuilder =
            Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), eventPerformanceTuple)
                .withLoggingEnabled(changeLogConfigs);

    // Topology
    Topology topology = new Topology();
    topology
        .addSource(
            "Txn-Source", stringDeserializer, stockTransactionDeserializer, "stock-transactions")
        .addSource("Events-Source", stringDeserializer, clickEventDeserializer, "events")
        .addProcessor("Txn-Processor", StockTransactionProcessor::new, "Txn-Source")
        .addProcessor("Events-Processor", ClickEventProcessor::new, "Events-Source")
        .addProcessor(
            "CoGrouping-Processor", CogroupingProcessor::new, "Txn-Processor", "Events-Processor")
        .addStateStore(storeBuilder, "CoGrouping-Processor")
        .addSink(
            "Tuple-Sink",
            "cogrouped-results",
            stringSerializer,
            tupleSerializer,
            "CoGrouping-Processor");

    topology.addProcessor("Print", new KStreamPrinter("Co-Grouping"), "CoGrouping-Processor");

    MockDataProducer.produceStockTransactionsAndDayTradingClickEvents(
        50, 100, 100, StockTransaction::getSymbol);

    KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);
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
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
    return props;
  }
}
