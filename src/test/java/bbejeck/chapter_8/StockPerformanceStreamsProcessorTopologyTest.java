package bbejeck.chapter_8;

import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import bbejeck.util.datagen.DataGenerator;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * User: Bill Bejeck
 * Date: 9/10/17
 * Time: 4:36 PM
 */
public class StockPerformanceStreamsProcessorTopologyTest {

    private  ProcessorTopologyTestDriver topologyTestDriver;

    @BeforeEach
    public void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "ks-papi-stock-analysis-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ks-papi-stock-analysis-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-stock-analysis-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        StreamsConfig streamsConfig = new StreamsConfig(props);

        Topology topology = StockPerformanceStreamsProcessorTopology.build();

        topologyTestDriver = new ProcessorTopologyTestDriver(streamsConfig, topology);
    }


    @Test
    @DisplayName("Checking State Store for Value")
    public void shouldStorePerformanceObjectInStore() {

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();

        StockTransaction stockTransaction = DataGenerator.generateStockTransaction();

        topologyTestDriver.process("stock-transactions",
                stockTransaction.getSymbol(),
                stockTransaction,
                stringSerde.serializer(),
                stockTransactionSerde.serializer());

        KeyValueStore<String, StockPerformance> store = topologyTestDriver.getKeyValueStore("stock-performance-store");
        
        assertThat(store.get(stockTransaction.getSymbol()), notNullValue());

        StockPerformance stockPerformance = store.get(stockTransaction.getSymbol());
        
        assertThat(stockPerformance.getCurrentShareVolume(), equalTo(stockTransaction.getShares()));
        assertThat(stockPerformance.getCurrentPrice(), equalTo(stockTransaction.getSharePrice()));
    }
}
