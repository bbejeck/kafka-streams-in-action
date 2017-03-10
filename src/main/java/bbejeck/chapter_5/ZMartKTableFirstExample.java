package bbejeck.chapter_5;


import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.Purchase;
import bbejeck.model.PurchasePattern;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

public class ZMartKTableFirstExample {

    public static void main(String[] args) throws Exception {
        //Used only to produce data for this application, not typical usage
        MockDataProducer.producePurchaseData();

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();


        Serde<String> stringSerde = Serdes.String();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KStream<String, Purchase> purchaseKStream = kStreamBuilder.stream(stringSerde, purchaseSerde, "transactions")
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());


        KeyValueMapper<String, PurchasePattern, KeyValue<String, PurchasePattern>> keyValueMapper = (key, pattern) -> new KeyValue<>(pattern.getZipCode(), pattern);


        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());

        //patternKStream.print("patterns");
        patternKStream.map(keyValueMapper).to(stringSerde, purchasePatternSerde, "patterns");


        KTable<String, PurchasePattern> purchasesTotalByZipCode = kStreamBuilder.table(TopologyBuilder.AutoOffsetReset.LATEST, stringSerde, purchasePatternSerde, "patterns", "purchases-by-zip-table");

        Initializer<Double> initializer = () -> 0.0;
        Aggregator<String, PurchasePattern, Double> amountAdd = (key, pattern, aggregate) -> {

            System.out.println("key " + key + " new value " + pattern.getAmount() + " aggregate " + aggregate);
            return aggregate + pattern.getAmount();
        };

        Aggregator<String, PurchasePattern, Double> amountSubtract = (key, pattern, aggregate) -> {
            System.out.println("key " + key + " old value " + pattern.getAmount() + " aggregate " + aggregate);

            return aggregate - pattern.getAmount();
        };


        purchasesTotalByZipCode.groupBy(keyValueMapper, stringSerde, purchasePatternSerde)
                .aggregate(initializer, amountAdd, amountSubtract, Serdes.Double(), "sales-by-zip-table").toStream()
                .print(stringSerde, Serdes.Double(), "total-sales by zip");


        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, streamsConfig);
        System.out.println("ZMart First Kafka Streams Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        System.out.println("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KTable-aggregations");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KTable-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KTable-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }

}
