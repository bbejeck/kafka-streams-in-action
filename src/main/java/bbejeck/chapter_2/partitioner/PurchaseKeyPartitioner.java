package bbejeck.chapter_2.partitioner;

import bbejeck.model.PurchaseKey;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;


public class PurchaseKeyPartitioner extends DefaultPartitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        if (key != null) {
            PurchaseKey purchaseKey = (PurchaseKey) key;
            key = purchaseKey.getCustomerId();
            keyBytes = ((String) key).getBytes();
        }
        return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
    }
}
