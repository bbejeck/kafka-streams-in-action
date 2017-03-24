package bbejeck.chapter_5;


import bbejeck.chapter_5.timestamp_extractor.StockTransactionTimestampExtractor;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.StockTransaction;
import bbejeck.model.TransactionSummary;
import bbejeck.util.datagen.CustomDateGenerator;
import bbejeck.util.datagen.DataGenerator;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.time.Duration;
import java.util.Properties;

import static bbejeck.clients.producer.MockDataProducer.STOCK_TOPIC;
import static org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset.EARLIEST;
import static org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset.LATEST;

public class GlobalKTableExample {

    public static void main(String[] args) throws Exception {


        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionKeySerde = StreamsSerdes.TransactionSummarySerde();


        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        long twentySeconds = 1000 * 20;

        KeyValueMapper<Windowed<TransactionSummary>, Long, KeyValue<String, TransactionSummary>> transactionMapper = (window, count) -> {
            TransactionSummary transactionSummary = window.key();
            String newKey = transactionSummary.getIndustry();
            transactionSummary.setSummaryCount(count);
            return KeyValue.pair(newKey, transactionSummary);
        };

        KStream<String, TransactionSummary> countStream =
                kStreamBuilder.stream(LATEST, stringSerde, transactionSerde, STOCK_TOPIC)
                        .groupBy((noKey, transaction) -> TransactionSummary.from(transaction), transactionKeySerde, transactionSerde)
                        .count(SessionWindows.with(twentySeconds), "session-windowed-customer-transaction-counts")
                        .toStream().map(transactionMapper);

        GlobalKTable<String, String> publicCompanies = kStreamBuilder.globalTable("companies", "global-company-store");
        GlobalKTable<String, String> clients = kStreamBuilder.globalTable("clients", "global-clients-store");


        countStream.leftJoin(publicCompanies,
                (key, txn) -> txn.getStockTicker(),
                TransactionSummary::withCompmanyName)
                .leftJoin(clients, (key, txn) -> txn.getCustomerId(), TransactionSummary::withCustomerName);


        
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, streamsConfig);
        kafkaStreams.cleanUp();


        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
            System.out.println("had exception " + e);
            e.printStackTrace();
        });
        CustomDateGenerator dateGenerator = CustomDateGenerator.withTimestampsIncreasingBy(Duration.ofMillis(750));

        DataGenerator.setTimestampGenerator(() -> dateGenerator.get());

        MockDataProducer.produceStockTransactions(2, 5, 3);

        System.out.println("Starting Aggregation and Joins Example");
        kafkaStreams.start();
        Thread.sleep(65000);
        System.out.println("Shutting down the Reduction and Aggregation Example Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Global_Ktable_example");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Global_Ktable_example_group_id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Global_Ktable_example_client_id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, StockTransactionTimestampExtractor.class);
        return props;

    }

}
