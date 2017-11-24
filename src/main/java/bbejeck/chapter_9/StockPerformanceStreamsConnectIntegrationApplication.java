package bbejeck.chapter_9;


import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StockPerformanceStreamsConnectIntegrationApplication {


    public static void main(String[] args) throws Exception {


        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Serde<String> stringSerde = Serdes.String();
        Serde<StockPerformance> stockPerformanceSerde = StreamsSerdes.StockPerformanceSerde();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();



        StreamsBuilder builder = new StreamsBuilder();



        builder.stream("dbTxnTRANSACTIONS",  Consumed.with(stringSerde, stockTransactionSerde))
                      .print(Printed.<String, StockTransaction>toSysOut().withLabel("Transaction"));

        //Uncomment this line and comment out the line above for writing to a topic
        //.to(stringSerde, stockPerformanceSerde, "stock-performance");


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        CountDownLatch doneSignal = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            doneSignal.countDown();
            System.out.println("Shutting down the Stock Analysis KStream Connect App Started now");
            kafkaStreams.close();
        }));

        System.out.println("Stock Analysis KStream Connect App Started");
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
