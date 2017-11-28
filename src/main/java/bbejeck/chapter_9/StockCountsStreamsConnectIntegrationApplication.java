package bbejeck.chapter_9;


import bbejeck.model.StockTransaction;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StockCountsStreamsConnectIntegrationApplication {

    private static final Logger LOG = LoggerFactory.getLogger(StockCountsStreamsConnectIntegrationApplication.class);

    public static void main(String[] args) throws Exception {


        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<Long> longSerde = Serdes.Long();
        
        StreamsBuilder builder = new StreamsBuilder();



        builder.stream("dbTxnTRANSACTIONS",  Consumed.with(stringSerde, stockTransactionSerde))
                      .peek((k, v)-> LOG.info("transactions from database key {} value {}",k, v))
                      .groupByKey(Serialized.with(stringSerde, stockTransactionSerde))
                      .aggregate(()-> 0L,(symb, stockTxn, numShares) -> numShares + stockTxn.getShares(),
                              Materialized.with(stringSerde, longSerde)).toStream()
                             .peek((k,v) -> LOG.info("Aggregated stock sales for {} {}",k, v))
                             .to( "stock-counts", Produced.with(stringSerde, longSerde));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        CountDownLatch doneSignal = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            doneSignal.countDown();
            LOG.info("Shutting down the Stock Analysis KStream Connect App Started now");
            kafkaStreams.close();
        }));

        
        LOG.info("Stock Analysis KStream Connect App Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        doneSignal.await();

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "ks-connect-stock-analysis-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ks-connect-stock-analysis-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-connect-stock-analysis-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeserializerErrorHandler.class);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG), Collections.singletonList(bbejeck.chapter_7.interceptors.StockTransactionConsumerInterceptor.class));
        return props;
    }
}
