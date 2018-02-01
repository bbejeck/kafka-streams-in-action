package bbejeck.chapter_7;


import bbejeck.chapter_7.transformer.StockPerformanceMetricsTransformer;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class StockPerformanceStreamsAndProcessorMetricsApplication {

    private static final Logger LOG = LoggerFactory.getLogger(StockPerformanceStreamsAndProcessorMetricsApplication.class);

    public static void main(String[] args) throws Exception {


        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Serde<String> stringSerde = Serdes.String();
        Serde<StockPerformance> stockPerformanceSerde = StreamsSerdes.StockPerformanceSerde();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();


        StreamsBuilder builder = new StreamsBuilder();

        String stocksStateStore = "stock-performance-store";
        double differentialThreshold = 0.05;

        KeyValueBytesStoreSupplier storeSupplier = Stores.lruMap(stocksStateStore, 100);
        StoreBuilder<KeyValueStore<String, StockPerformance>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), stockPerformanceSerde);

        builder.addStateStore(storeBuilder);

        builder.stream("stock-transactions", Consumed.with(stringSerde, stockTransactionSerde))
                .transform(() -> new StockPerformanceMetricsTransformer(stocksStateStore, differentialThreshold), stocksStateStore)
                .peek((k, v)-> LOG.info("[stock-performance] key: {} value: {}" , k, v))
                .to( "stock-performance", Produced.with(stringSerde, stockPerformanceSerde));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        MockDataProducer.produceStockTransactionsWithKeyFunction(50, 50, 25, StockTransaction::getSymbol);
        LOG.info("Stock Analysis KStream/Process API Metrics App Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();




        Thread.sleep(70000);


        LOG.info("Shutting down the Stock KStream/Process API Analysis Metrics  App now");
        for (Map.Entry<MetricName, ? extends Metric> metricNameEntry :kafkaStreams.metrics().entrySet()) {
            Metric metric = metricNameEntry.getValue();
            MetricName metricName = metricNameEntry.getKey();
            if(!metric.metricValue().equals(0.0) && !metric.metricValue().equals(Double.NEGATIVE_INFINITY)) {
                LOG.info("MetricName {}", metricName.name());
                LOG.info(" = {}", metric.metricValue());
            }   

        }
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "ks-stats-stock-analysis-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ks-stats-stock-analysis-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-stats-stock-analysis-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG), Collections.singletonList(bbejeck.chapter_7.interceptors.StockTransactionConsumerInterceptor.class));
        return props;
    }
}
