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

package bbejeck.chapter_7;

import bbejeck.chapter_3.service.SecurityDBService;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.Purchase;
import bbejeck.model.PurchasePattern;
import bbejeck.model.RewardAccumulator;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ZMartKafkaStreamsAdvancedReqsMetricsApp {

    public static void main(String[] args) throws Exception {

        //Used only to produce data for this application, not typical usage
        MockDataProducer.producePurchaseData();

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder streamsBuilder = new StreamsBuilder();


        /**
         * Previous requirements
         */
        KStream<String,Purchase> purchaseKStream = streamsBuilder.stream(stringSerde, purchaseSerde, "transactions")
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.print(stringSerde, purchasePatternSerde, "patterns");
        patternKStream.to(stringSerde,purchasePatternSerde,"patterns");


        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build());

        rewardsKStream.print(stringSerde,rewardAccumulatorSerde,"rewards");
        rewardsKStream.to(stringSerde,rewardAccumulatorSerde,"rewards");


        /**
         *  Selecting a key for storage and filtering out low dollar purchases
         */

        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase.getPurchaseDate().getTime();

        KStream<Long, Purchase> filteredKStream = purchaseKStream.filter((key, purchase) -> purchase.getPrice() > 5.00).selectKey(purchaseDateAsKey);

        filteredKStream.print(Serdes.Long(),purchaseSerde,"purchases");
        filteredKStream.to(Serdes.Long(),purchaseSerde,"purchases");


        /**
         * Branching stream for separating out purchases in new departments to their own topics
         */
        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        int coffee = 0;
        int electronics = 1;

        KStream<String, Purchase>[] kstreamByDept = purchaseKStream.branch(isCoffee, isElectronics);

        kstreamByDept[coffee].to(stringSerde, purchaseSerde, "coffee");
        kstreamByDept[coffee].print(stringSerde, purchaseSerde, "coffee");

        kstreamByDept[electronics].to(stringSerde, purchaseSerde, "electronics");
        kstreamByDept[electronics].print(stringSerde, purchaseSerde, "electronics");



        /**
         * Security Requirements to record transactions for certain employee
         */
        ForeachAction<String, Purchase> purchaseForeachAction = (key, purchase) ->
                SecurityDBService.saveRecord(purchase.getPurchaseDate(), purchase.getEmployeeId(), purchase.getItemPurchased());

        
        purchaseKStream.filter((key, purchase) -> purchase.getEmployeeId().equals("000000")).foreach(purchaseForeachAction);

        Topology topology = streamsBuilder.build();


        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);


        System.out.println("ZMart Advanced Requirements Metrics Application Started");
        kafkaStreams.cleanUp();
        CountDownLatch stopSignal = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            System.out.println("Shutting down the Kafka Streams Application now");
            kafkaStreams.close();
            MockDataProducer.shutdown();
            stopSignal.countDown();
        }));



        kafkaStreams.start();
        Thread.sleep(10000);
        System.out.println(topology.describe());
        System.out.println(kafkaStreams.toString());

        stopSignal.await();
        System.out.println("All done now, good-bye");
    }




    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "zmart-metrics-client-id");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-metrics-group-id");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart-metrics-application-id");
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }

}
