package bbejeck.clients.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Properties;

/**
 * This class is used to specifically publish key-value pairs to the
 * specified topic(s) via a command line argument.  This Producer only sends String keys and
 * String values.
 *
 * This class uses a ':' to split the key value pair entered on the command line.
 *
 * In the book this class was used in the start of Chapter 5 to demonstrate difference in
 * processing between a KTable and a KStream
 *
 * To stop, type quit.
 */
public class KeyValueMultiTopicConsoleProducer {


    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.out.println("Please specify a topic or comma separated list of topics");
            System.exit(1);
        }

        String[] topics = args[0].split(",");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        Callback callback = (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            }
        };

        System.out.println("This is a key-value command line producer");
        System.out.println("Enter messages in key:value format, type 'quit' to exit");
        System.out.println("Sending messages to topics "+ Arrays.toString(topics));

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String line = reader.readLine();

        while (!(line.equalsIgnoreCase("quit"))) {

            String[] keyValue = line.split(":");
            String key = keyValue[0];
            String value = keyValue[1];

            for (String topic : topics) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record, callback);
            }
            line = reader.readLine();
        }

    }


}
