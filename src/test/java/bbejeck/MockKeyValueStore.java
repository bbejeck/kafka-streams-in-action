package bbejeck;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.KeyValueIteratorStub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MockKeyValueStore<K, V> implements KeyValueStore<K, V> {

    protected   Map<K, V> inner = new HashMap<>();
    private String name = "mockStore";
    private boolean open = true;

    public MockKeyValueStore () {}


    @Override
    public void put(K key, V value) {
         inner.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        if (!inner.containsKey(key)) {
             inner.put(key, value);
        }
        return value;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries) {
             inner.put(entry.key, entry.value);
        }
    }

    @Override
    public V delete(K key) {
        return inner.remove(key);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public V get(K key) {
        return inner.get(key);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return null;
    }

    @Override
    public KeyValueIterator<K, V> all() {
        List<KeyValue<K, V>> entryList = new ArrayList<>();
        for (Map.Entry<K, V> tupleEntry : inner.entrySet()) {
            entryList.add(KeyValue.pair(tupleEntry.getKey(),tupleEntry.getValue()));
        }
        return new KeyValueIteratorStub<>(entryList.iterator());
    }

    @Override
    public long approximateNumEntries() {
        return inner.size();
    }

    public Map<K, V> innerStore() {
        return inner;
    }
}
