package bbejeck.chapter_6;


import bbejeck.chapter_6.processor.BeerPurchaseProcessor;
import bbejeck.model.BeerPurchase;
import bbejeck.util.serializer.JsonDeserializer;
import bbejeck.util.serializer.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import static org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset.*;

import java.util.Properties;

public class PopsHopsApplication {


    public static void main(String[] args) {

        Deserializer<BeerPurchase> beerPurchaseDeserializer = new JsonDeserializer<>(BeerPurchase.class);
        Serializer<BeerPurchase> beerPurchaseSerializer = new JsonSerializer<>();
        Serde<String> stringSerde = Serdes.String();
        Serializer<String> stringSerializer = stringSerde.serializer();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();

        TopologyBuilder builder = new TopologyBuilder();

        String domesticSalesSink = "domestic-beer-sales";
        String internationalSalesSink = "international-beer-sales";
        String purchaseSourceNode = "beer-purchase-source";
        String purchaseProcessor = "purchase-processor";


        BeerPurchaseProcessor beerProcessor = new BeerPurchaseProcessor(domesticSalesSink, internationalSalesSink);

        builder.addSource(LATEST, purchaseSourceNode, stringDeserializer, beerPurchaseDeserializer, "pops-hops-purchases")
                .addProcessor(purchaseProcessor, () -> beerProcessor, purchaseSourceNode)
                .addSink(internationalSalesSink,"international-sales", stringSerializer, beerPurchaseSerializer, purchaseProcessor)
                .addSink(domesticSalesSink,"domestic-sales", stringSerializer, beerPurchaseSerializer, purchaseProcessor);

    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "beer-app-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "beer-app-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "beer-app-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
