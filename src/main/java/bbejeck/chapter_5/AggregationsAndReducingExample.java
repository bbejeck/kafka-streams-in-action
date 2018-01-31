package bbejeck.chapter_5;


import bbejeck.clients.producer.MockDataProducer;
import bbejeck.collectors.FixedSizePriorityQueue;
import bbejeck.model.ShareVolume;
import bbejeck.model.StockTransaction;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

import static bbejeck.clients.producer.MockDataProducer.STOCK_TRANSACTIONS_TOPIC;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;


@SuppressWarnings("unchecked")
public class AggregationsAndReducingExample {

    private static Logger LOG = LoggerFactory.getLogger(AggregationsAndReducingExample.class);

    public static void main(String[] args) throws Exception {


        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<ShareVolume> shareVolumeSerde = StreamsSerdes.ShareVolumeSerde();
        Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = StreamsSerdes.FixedSizePriorityQueueSerde();
        NumberFormat numberFormat = NumberFormat.getInstance();
        
        Comparator<ShareVolume> comparator = (sv1, sv2) -> sv2.getShares() - sv1.getShares();
        FixedSizePriorityQueue<ShareVolume> fixedQueue = new FixedSizePriorityQueue<>(comparator, 5);

        ValueMapper<FixedSizePriorityQueue, String> valueMapper = fpq -> {
            StringBuilder builder = new StringBuilder();
            Iterator<ShareVolume> iterator = fpq.iterator();
            int counter = 1;
            while (iterator.hasNext()) {
                ShareVolume stockVolume = iterator.next();
                if (stockVolume != null) {
                    builder.append(counter++).append(")").append(stockVolume.getSymbol())
                            .append(":").append(numberFormat.format(stockVolume.getShares())).append(" ");
                }
            }
            return builder.toString();
        };

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, ShareVolume> shareVolume = builder.stream(STOCK_TRANSACTIONS_TOPIC,
                Consumed.with(stringSerde, stockTransactionSerde)
                        .withOffsetResetPolicy(EARLIEST))
                .mapValues(st -> ShareVolume.newBuilder(st).build())
                .groupBy((k, v) -> v.getSymbol(), Serialized.with(stringSerde, shareVolumeSerde))
                .reduce(ShareVolume::sum);


        shareVolume.groupBy((k, v) -> KeyValue.pair(v.getIndustry(), v), Serialized.with(stringSerde, shareVolumeSerde))
                .aggregate(() -> fixedQueue,
                        (k, v, agg) -> agg.add(v),
                        (k, v, agg) -> agg.remove(v),
                        Materialized.with(stringSerde, fixedSizePriorityQueueSerde))
                .mapValues(valueMapper)
                .toStream().peek((k, v) -> LOG.info("Stock volume by industry {} {}", k, v))
                .to("stock-volume-by-company", Produced.with(stringSerde, stringSerde));



        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        MockDataProducer.produceStockTransactions(15, 50, 25, false);
        LOG.info("First Reduction and Aggregation Example Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Reduction and Aggregation Example Application now");
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
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }

}
