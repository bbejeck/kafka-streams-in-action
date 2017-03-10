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

package bbejeck.chapter_3;

import bbejeck.clients.producer.MockDataProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class KafkaStreamsYellingApp {

    public static void main(String[] args) throws Exception {


        //Used only to produce data for this application, not typical usage
        MockDataProducer.produceRandomTextData();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamsConfig = new StreamsConfig(props);

        Serde<String> stringSerde = Serdes.String();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KStream<String, String> simpleFirstStream = kStreamBuilder.stream(stringSerde, stringSerde, "src-topic");


        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues(String::toUpperCase);

        upperCasedStream.to(stringSerde, stringSerde, "out-topic");
        upperCasedStream.print("Yelling App");


        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,streamsConfig);
        System.out.println("Hello World Yelling App Started");
        kafkaStreams.start();
        Thread.sleep(35000);
        System.out.println("Shutting down the Yelling APP now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }
}