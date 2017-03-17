package bbejeck.chapter_5;


import bbejeck.clients.producer.MockDataProducer;
import bbejeck.collectors.FixedSizePriorityQueue;
import bbejeck.model.ShareVolume;
import bbejeck.model.StockTransaction;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

import static bbejeck.clients.producer.MockDataProducer.STOCK_TOPIC;
import static org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset.EARLIEST;

public class AggregationsAndReducingExample {

    public static void main(String[] args) throws Exception {


        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<ShareVolume> shareVolumeSerde = StreamsSerdes.ShareVolumeSerde();
        Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = StreamsSerdes.FixedSizePriorityQueueSerde();
        NumberFormat numberFormat = NumberFormat.getInstance();
        
        Comparator<ShareVolume> comparator = (st1, st2) -> st2.getShares() - st1.getShares();
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

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KTable<String, ShareVolume> shareVolume = kStreamBuilder.stream(EARLIEST, stringSerde, stockTransactionSerde, STOCK_TOPIC)
                .mapValues(st -> ShareVolume.newBuilder(st).build())
                .groupBy((k, v) -> v.getSymbol(), stringSerde, shareVolumeSerde)
                .reduce(ShareVolume::reduce, "stock-transaction-reductions");


        shareVolume.groupBy((k, v) -> KeyValue.pair(v.getIndustry(), v), stringSerde, shareVolumeSerde)
                .aggregate(() -> fixedQueue,
                        (k, v, agg) -> agg.add(v),
                        (k, v, agg) -> agg.remove(v),
                        fixedSizePriorityQueueSerde,
                        "volume-shares-industry-store")
                .mapValues(valueMapper)
                //.to("stock-volume-by-company")
                .print("Stock volume by Industry");


        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, streamsConfig);
        MockDataProducer.produceStockTransactions(15);
        System.out.println("First Reduction and Aggregation Example Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        System.out.println("Shutting down the Reduction and Aggregation Example Application now");
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
