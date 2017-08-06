package bbejeck.chapter_5;


import bbejeck.chapter_5.timestamp_extractor.StockTransactionTimestampExtractor;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.StockTransaction;
import bbejeck.model.TransactionSummary;
import bbejeck.util.datagen.DataGenerator;
import bbejeck.util.datagen.CustomDateGenerator;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.time.Duration;
import java.util.Properties;

import static bbejeck.clients.producer.MockDataProducer.STOCK_TRANSACTIONS_TOPIC;
import static org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset.EARLIEST;
import static org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset.LATEST;

public class CountingWindowingAndKtableJoinExample {

    public static void main(String[] args) throws Exception {


        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionKeySerde = StreamsSerdes.TransactionSummarySerde();

        WindowedSerializer<TransactionSummary> windowedSerializer = new WindowedSerializer<>(transactionKeySerde.serializer());
        WindowedDeserializer<TransactionSummary> windowedDeserializer = new WindowedDeserializer<>(transactionKeySerde.deserializer());
        Serde<Windowed<TransactionSummary>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        long twentySeconds = 1000 * 20;
        long fifteenMinutes = 1000 * 60 * 15;
        long fiveSeconds = 1000 * 5;
        KTable<Windowed<TransactionSummary>, Long> customerTransactionCounts =
                 kStreamBuilder.stream(LATEST, stringSerde, transactionSerde, STOCK_TRANSACTIONS_TOPIC)
                .groupBy((noKey, transaction) -> TransactionSummary.from(transaction), transactionKeySerde, transactionSerde)
                .count(SessionWindows.with(twentySeconds).until(fifteenMinutes),"session-windowed-customer-transaction-counts");

                //The following are examples of different windows examples

                //Tumbling window with timeout 15 minutes
                //.count(TimeWindows.of(twentySeconds).until(fifteenMinutes),"tumbling-windowed-customer-transaction-counts");

                //Tumbling window with default timeout 24 hours
                //.count(TimeWindows.of(twentySeconds),"tumbling-windowed-customer-transaction-counts");

                //Hopping window 
                //.count(TimeWindows.of(twentySeconds).advanceBy(fiveSeconds).until(fifteenMinutes),"hopping-windowed-customer-transaction-counts");

        customerTransactionCounts.toStream().print(windowedSerde, Serdes.Long(),"Customer Transactions Counts");

        KStream<String, TransactionSummary> countStream = customerTransactionCounts.toStream().map((window, count) -> {
                      TransactionSummary transactionSummary = window.key();
                      String newKey = transactionSummary.getIndustry();
                      transactionSummary.setSummaryCount(count);
                      return KeyValue.pair(newKey, transactionSummary);
        });

        KTable<String, String> financialNews = kStreamBuilder.table(EARLIEST, "financial-news", "financial-news-store");


        ValueJoiner<TransactionSummary, String, String> valueJoiner = (txnct, news) ->
                String.format("%d shares purchased %s related news [%s]", txnct.getSummaryCount(), txnct.getStockTicker(), news);

        KStream<String,String> joined = countStream.leftJoin(financialNews, valueJoiner, stringSerde, transactionKeySerde);

        joined.print("Transactions and News");



        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, streamsConfig);
        kafkaStreams.cleanUp();
        
        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
            System.out.println("had exception "+e);
            e.printStackTrace();
        });
        CustomDateGenerator dateGenerator = CustomDateGenerator.withTimestampsIncreasingBy(Duration.ofMillis(750));
        
        DataGenerator.setTimestampGenerator(() -> dateGenerator.get());
        
        MockDataProducer.produceStockTransactions(2, 5, 3, false);

        System.out.println("Starting CountingWindowing and KTableJoins Example");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(65000);
        System.out.println("Shutting down the CountingWindowing and KTableJoins Example Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Windowing-counting-ktable-joins");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Windowing-counting-ktable-joins");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Windowing-counting-ktable-joins");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, StockTransactionTimestampExtractor.class);
        return props;

    }

}
