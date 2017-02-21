package bbejeck.util.serde;

import bbejeck.model.Purchase;
import bbejeck.model.PurchasePattern;
import bbejeck.model.RewardAccumulator;
import bbejeck.util.serializer.JsonDeserializer;
import bbejeck.util.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class StreamsSerdes {

    public static Serde<PurchasePattern> PurchasePatternSerde() {
             return new PurchasePatternsSerde();
    }

    public static Serde<RewardAccumulator> RewardAccumulatorSerde() {
        return new RewardAccumulatorSerde();
    }

    public static Serde<Purchase> PurchaseSerde() {
        return new PurchaseSerde();
    }


    private static final class PurchasePatternsSerde extends WrapperSerde<PurchasePattern> {
        private PurchasePatternsSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(PurchasePattern.class));
        }
    }

    private static final class RewardAccumulatorSerde extends WrapperSerde<RewardAccumulator> {
        private RewardAccumulatorSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    private static final class PurchaseSerde extends WrapperSerde<Purchase> {
        private PurchaseSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    private static class WrapperSerde<T> implements Serde<T> {

        private JsonSerializer<T> serializer;
        private JsonDeserializer<T> deserializer;

        public WrapperSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<T> serializer() {
           return serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
           return deserializer;
        }
    }
}
