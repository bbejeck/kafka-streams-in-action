/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bbejeck.chapter_4;

import bbejeck.model.Purchase;
import bbejeck.model.PurchasePattern;
import bbejeck.model.RewardAccumulator;
import bbejeck.chapter_4.partitioner.RewardsStreamPartitioner;
import bbejeck.util.serde.StreamsSerdes;
import bbejeck.chapter_4.transformer.PurchaseRewardTransformer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;


public class ZMartKafkaStreamsAddStateApp {

    public static void main(String[] args) {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();



        KStream<String,Purchase> purchaseKStream = kStreamBuilder.stream(stringSerde, purchaseSerde, "transactions")
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build()).to(stringSerde,purchasePatternSerde,"patterns");

        /**
         *  Adding State to processor
         */

        String rewardsStateStoreName = "rewardsPointsStore";
        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();
        PurchaseRewardTransformer transformer = new PurchaseRewardTransformer(rewardsStateStoreName);


        KStream<String, Purchase> transByCustomerStream = purchaseKStream.through(stringSerde, purchaseSerde, streamPartitioner, "customer_transactions");

        transByCustomerStream.transformValues(() -> transformer).to(stringSerde, rewardAccumulatorSerde, "rewards");


        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,streamsConfig);
        kafkaStreams.start();
    }




    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "AddingStateConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "AddingStateGroupId");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AddingStateAppId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

}
