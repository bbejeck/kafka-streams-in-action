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
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static bbejeck.clients.producer.MockDataProducer.STOCK_TRANSACTIONS_TOPIC;
import static bbejeck.util.Topics.CLIENTS;
import static bbejeck.util.Topics.COMPANIES;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;

public class GlobalKTableExample {

    private static Logger LOG = LoggerFactory.getLogger(GlobalKTableExample.class);

    public static void main(String[] args) throws Exception {


        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionSummarySerde = StreamsSerdes.TransactionSummarySerde();


        StreamsBuilder builder = new StreamsBuilder();
        long twentySeconds = 1000 * 20;

        KeyValueMapper<Windowed<TransactionSummary>, Long, KeyValue<String, TransactionSummary>> transactionMapper = (window, count) -> {
            TransactionSummary transactionSummary = window.key();
            String newKey = transactionSummary.getIndustry();
            transactionSummary.setSummaryCount(count);
            return KeyValue.pair(newKey, transactionSummary);
        };

        KStream<String, TransactionSummary> countStream =
                builder.stream( STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde, transactionSerde).withOffsetResetPolicy(LATEST))
                        .groupBy((noKey, transaction) -> TransactionSummary.from(transaction), Serialized.with(transactionSummarySerde, transactionSerde))
                        .windowedBy(SessionWindows.with(twentySeconds)).count()
                        .toStream().map(transactionMapper);

        GlobalKTable<String, String> publicCompanies = builder.globalTable(COMPANIES.topicName());
        GlobalKTable<String, String> clients = builder.globalTable(CLIENTS.topicName());


        countStream.leftJoin(publicCompanies, (key, txn) -> txn.getStockTicker(),TransactionSummary::withCompanyName)
                .leftJoin(clients, (key, txn) -> txn.getCustomerId(), TransactionSummary::withCustomerName)
                .print(Printed.<String, TransactionSummary>toSysOut().withLabel("Resolved Transaction Summaries"));


        
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.cleanUp();


        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
            LOG.error("had exception ", e);
        });

        CustomDateGenerator dateGenerator = CustomDateGenerator.withTimestampsIncreasingBy(Duration.ofMillis(750));

        DataGenerator.setTimestampGenerator(dateGenerator::get);

        MockDataProducer.produceStockTransactions(2, 5, 3, true);

        LOG.info("Starting GlobalKTable Example");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the GlobalKTable Example Application now");
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
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, StockTransactionTimestampExtractor.class);
        return props;

    }

}
