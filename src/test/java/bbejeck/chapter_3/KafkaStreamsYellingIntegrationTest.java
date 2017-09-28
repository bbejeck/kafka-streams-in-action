package bbejeck.chapter_3;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;


public class KafkaStreamsYellingIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();
    private final MockTime mockTime = new MockTime();

    private KafkaStreams kafkaStreams;
    private StreamsConfig streamsConfig;
    private static final String YELL_A_TOPIC = "yell-A-topic";
    private static final String YELL_B_TOPIC = "yell-B-topic";
    private static final String OUT_TOPIC = "out-topic";


    @ClassRule
    public static final EmbeddedKafkaCluster EMBEDDED_KAFKA = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void setUpAll() throws Exception {
        EMBEDDED_KAFKA.createTopic(YELL_A_TOPIC);
        EMBEDDED_KAFKA.createTopic(OUT_TOPIC);
    }


    @Before
    public void setUp() {
        Properties properties = StreamsTestUtils.getStreamsConfig("integrationTest",
                EMBEDDED_KAFKA.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                new Properties());
        properties.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfig  = new StreamsConfig(properties);
    }

    @After
    public void tearDown() {
        if (kafkaStreams!=null){
            kafkaStreams.close();
        }
    }


    @Test
    public void shouldYellFromMultipleTopics() throws Exception {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

         streamsBuilder.<String,String>stream(Pattern.compile("yell.*"))
                 .mapValues(String::toUpperCase)
                 .to(OUT_TOPIC);

        kafkaStreams = new KafkaStreams(streamsBuilder.build(),streamsConfig);
        kafkaStreams.start();

        final Properties producerConfig = TestUtils.producerConfig(EMBEDDED_KAFKA.bootstrapServers(), StringSerializer.class, StringSerializer.class);
        final Properties consumerConfig = TestUtils.consumerConfig(EMBEDDED_KAFKA.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        produceValues(YELL_A_TOPIC, Arrays.asList("this","should","yell","at","you"), producerConfig);
        List<String> expectedReceivedValues = Arrays.asList("THIS", "SHOULD", "YELL", "AT", "YOU");
        List<String> actualValues = readValues(OUT_TOPIC,5, consumerConfig);

        assertThat(actualValues, equalTo(expectedReceivedValues));

        EMBEDDED_KAFKA.createTopic(YELL_B_TOPIC);

        produceValues(YELL_B_TOPIC, Arrays.asList("yell","at","you","too"), producerConfig);
        expectedReceivedValues = Arrays.asList("YELL", "AT", "YOU", "TOO");
        actualValues = readValues(OUT_TOPIC,4, consumerConfig);

        assertThat(actualValues, equalTo(expectedReceivedValues));

    }

    private void produceValues(String topic, List<String> toSend, Properties producerConfig) throws  Exception {
        IntegrationTestUtils.produceValuesSynchronously(topic, toSend, producerConfig, mockTime);
    }

    private List<String> readValues(String topic, int expectedSize, Properties consumerConfig) throws Exception {
        List<KeyValue<String, String>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, expectedSize);
        List<String> actualValues = new ArrayList<>(expectedSize);

        for (final KeyValue<String, String> receivedKeyValue : receivedKeyValues) {
            actualValues.add(receivedKeyValue.value);
        }
        return actualValues;
    }
}
