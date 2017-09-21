package bbejeck.chapter_8;

import bbejeck.model.Purchase;
import bbejeck.model.PurchasePattern;
import bbejeck.model.RewardAccumulator;
import bbejeck.util.datagen.DataGenerator;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * User: Bill Bejeck
 * Date: 9/9/17
 * Time: 2:39 PM
 */
public class ZMartTopologyTest {

    private  ProcessorTopologyTestDriver topologyTestDriver;

    @BeforeEach
    public  void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        StreamsConfig streamsConfig = new StreamsConfig(props);
        Topology topology = ZMartTopology.build();
        
        topologyTestDriver = new ProcessorTopologyTestDriver(streamsConfig, topology);
    }


    @Test
    @DisplayName("Testing the ZMart Topology Flow")
    public void testZMartTopology() {

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        Purchase purchase = DataGenerator.generatePurchase();

        topologyTestDriver.process("transactions",
                null,
                purchase,
                stringSerde.serializer(),
                purchaseSerde.serializer());

        ProducerRecord<String, Purchase> record = topologyTestDriver.readOutput("purchases",
                stringSerde.deserializer(),
                purchaseSerde.deserializer());

        Purchase expectedPurchase = Purchase.builder(purchase).maskCreditCard().build();
        assertThat(record.value(), equalTo(expectedPurchase));


        RewardAccumulator expectedRewardAccumulator = RewardAccumulator.builder(expectedPurchase).build();

        ProducerRecord<String, RewardAccumulator> accumulatorProducerRecord = topologyTestDriver.readOutput("rewards",
                stringSerde.deserializer(),
                rewardAccumulatorSerde.deserializer());

        assertThat(accumulatorProducerRecord.value(), equalTo(expectedRewardAccumulator));

        PurchasePattern expectedPurchasePattern = PurchasePattern.builder(expectedPurchase).build();

        ProducerRecord<String, PurchasePattern> purchasePatternProducerRecord = topologyTestDriver.readOutput("patterns",
                stringSerde.deserializer(),
                purchasePatternSerde.deserializer());

        assertThat(purchasePatternProducerRecord.value(), equalTo(expectedPurchasePattern));
    }
}
