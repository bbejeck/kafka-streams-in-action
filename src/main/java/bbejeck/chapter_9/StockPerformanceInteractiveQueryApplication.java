package bbejeck.chapter_9;


import bbejeck.chapter_9.restore.StateRestoreHttpReporter;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.CustomerTransactions;
import bbejeck.model.StockTransaction;
import bbejeck.util.serde.StreamsSerdes;
import bbejeck.webserver.InteractiveQueryServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StockPerformanceInteractiveQueryApplication {

    private static final Logger LOG = LoggerFactory.getLogger(StockPerformanceInteractiveQueryApplication.class);

    public static void main(String[] args) {

        if(args.length < 2){
            LOG.error("Need to specify host, port");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        final HostInfo hostInfo = new HostInfo(host, port);

        Properties properties = getProperties();
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host+":"+port);

        StreamsConfig streamsConfig = new StreamsConfig(properties);
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerde.serializer());
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringSerde.deserializer());
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);
        Serde<CustomerTransactions> customerTransactionsSerde = StreamsSerdes.CustomerTransactionsSerde();

        Aggregator<String, StockTransaction, Integer> sharesAggregator = (k, v, i) -> v.getShares() + i;

        StreamsBuilder builder = new StreamsBuilder();

        // data is already coming in keyed
        KStream<String, StockTransaction> stockTransactionKStream = builder.stream(MockDataProducer.STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde, stockTransactionSerde)
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));


        stockTransactionKStream.map((k,v) -> KeyValue.pair(v.getSector(), v))
                .groupByKey(Serialized.with(stringSerde, stockTransactionSerde))
                .count(Materialized.as("TransactionsBySector"))
                .toStream()
                .peek((k,v) -> LOG.info("Transaction count for {} {}", k, v))
                .to("sector-transaction-counts", Produced.with(stringSerde, longSerde));
        
        stockTransactionKStream.map((k,v) -> KeyValue.pair(v.getCustomerId(), v))
                .groupByKey(Serialized.with(stringSerde, stockTransactionSerde))
                .windowedBy(SessionWindows.with(TimeUnit.MINUTES.toMillis(60)).until(TimeUnit.MINUTES.toMillis(120)))
                .aggregate(CustomerTransactions::new,(k, v, ct) -> ct.update(v),
                        (k, ct, other)-> ct.merge(other),
                        Materialized.<String, CustomerTransactions, SessionStore<Bytes, byte[]>>as("CustomerPurchaseSessions")
                                .withKeySerde(stringSerde).withValueSerde(customerTransactionsSerde))
                .toStream()
                .peek((k,v) -> LOG.info("Session info for {} {}", k, v))
                .to("session-transactions", Produced.with(windowedSerde, customerTransactionsSerde));


        stockTransactionKStream.groupByKey(Serialized.with(stringSerde, stockTransactionSerde))
                .windowedBy(TimeWindows.of(10000))
                .aggregate(() -> 0, sharesAggregator,
                        Materialized.<String, Integer, WindowStore<Bytes, byte[]>>as("NumberSharesPerPeriod")
                                .withKeySerde(stringSerde)
                                .withValueSerde(Serdes.Integer()))
                .toStream().peek((k,v)->LOG.info("key is {} value is {}", k, v))
                .to("transaction-count", Produced.with(windowedSerde,Serdes.Integer()));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        InteractiveQueryServer queryServer = new InteractiveQueryServer(kafkaStreams, hostInfo);
        StateRestoreHttpReporter restoreReporter = new StateRestoreHttpReporter(queryServer);

        queryServer.init();

        kafkaStreams.setGlobalStateRestoreListener(restoreReporter);

        kafkaStreams.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                LOG.info("Setting the query server to ready");
                queryServer.setReady(true);
            } else if (newState != KafkaStreams.State.RUNNING) {
                LOG.info("State not RUNNING, disabling the query server");
                queryServer.setReady(false);
            }
        }));

        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
            LOG.error("Thread {} had a fatal error {}", t, e, e);
            shutdown(kafkaStreams, queryServer);
        });


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown(kafkaStreams, queryServer);
        }));

        LOG.info("Stock Analysis KStream Interactive Query App Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }

    private static void shutdown(KafkaStreams kafkaStreams, InteractiveQueryServer queryServer) {
        LOG.info("Shutting down the Stock Analysis Interactive Query App Started now");
        kafkaStreams.close();
        queryServer.stop();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "ks-interactive-stock-analysis-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ks-interactive-stock-analysis-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-interactive-stock-analysis-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.topicPrefix("retention.bytes"), 1024 * 1024);
        props.put(StreamsConfig.topicPrefix("retention.ms"), 3600000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeserializerErrorHandler.class);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG), Collections.singletonList(bbejeck.chapter_7.interceptors.StockTransactionConsumerInterceptor.class));
        return props;
    }
}
