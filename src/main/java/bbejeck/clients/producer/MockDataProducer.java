package bbejeck.clients.producer;

import bbejeck.util.datagen.DataGenerator;
import bbejeck.model.Purchase;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 2/20/17
 * Time: 9:41 AM
 */
public class MockDataProducer {

    public static void main(String[] args)  throws Exception {

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

        System.out.println("This producer sends generated data to a specified topic");
        System.out.println("To send data enter a command in the following format class:number of generated objects:topic");
        System.out.println("Purchase:1000:transactions");
        System.out.println("Enter quit to exit");


        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String line = reader.readLine();

        while (!(line.equalsIgnoreCase("quit"))) {

            String[] keyValue = line.split(":");
            String dataClass = keyValue[0];
            int number = Integer.parseInt(keyValue[1]);
            String topic = keyValue[2];
            List<String> jsonMessages = null;

            switch (dataClass.toLowerCase()) {
                case "purchase":
                List<Purchase> purchases = DataGenerator.generatePurchases(number);
                jsonMessages = convertToJson(purchases);
            }


            if(jsonMessages == null) {
                throw new IllegalStateException("jsonMessages is null, no data has been generated");
            }

            for (String message : jsonMessages) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, message);
                producer.send(record, callback);
            }
            line = reader.readLine();
        }

    }


    private static <T> List<String> convertToJson(List<T> generatedDataItems) {
        Gson gson = new Gson();
        List<String> jsonList = new ArrayList<>();
        for (T generatedData : generatedDataItems) {
              jsonList.add(gson.toJson(generatedData));
        }
       return jsonList;
    }
}
