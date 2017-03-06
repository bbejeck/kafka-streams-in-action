package bbejeck.clients.consumer;


import java.util.Properties;
import java.util.regex.Pattern;

public class ConsumerProperties {

    private String keyDeserializer;
    private String valueDeserializer;
    private String topics;
    private Pattern topicPattern;
    private String offsetReset;
    private String bootstrapServers;
    private String groupId;

    private ConsumerProperties(Builder builder) {
        keyDeserializer = builder.keyDeserializer;
        valueDeserializer = builder.valueDeserializer;
        topics = builder.topics;
        topicPattern = builder.topicPattern;
        offsetReset = builder.offsetReset;
        bootstrapServers = builder.bootstrapServers;
        groupId = builder.groupId;
    }


    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public String getTopics() {
        return topics;
    }

    public Pattern getTopicPattern() {
        return topicPattern;
    }

    public String getOffsetReset() {
        return offsetReset;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", offsetReset);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "3000");
        properties.put("key.deserializer", keyDeserializer);
        properties.put("value.deserializer", valueDeserializer);

        return properties;

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String keyDeserializer;
        private String valueDeserializer;
        private String topics;
        private Pattern topicPattern;
        private String offsetReset = "latest";
        private String bootstrapServers = "localhost:9092";
        private String groupId;

        private Builder() {
        }

        public Builder withKeyDeserializer(String val) {
            keyDeserializer = val;
            return this;
        }

        public Builder withValueDeserializer(String val) {
            valueDeserializer = val;
            return this;
        }

        public Builder withTopics(String val) {
            topics = val;
            return this;
        }

        public Builder withTopicPattern(Pattern val) {
            topicPattern = val;
            return this;
        }

        public Builder withOffsetReset(String val) {
            offsetReset = val;
            return this;
        }

        public Builder withBootstrapServers(String val) {
            bootstrapServers = val;
            return this;
        }

        public Builder withGroupId(String val) {
            groupId = val;
            return this;
        }

        public ConsumerProperties build() {
            return new ConsumerProperties(this);
        }
    }
}
