package bbejeck.chapter_6.processor;

import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.function.Function;



public class MapValueProcessor<K, V, VR> extends AbstractProcessor<K, V> {

    // It is assumed the valueMapper is stateless
    private Function<V, VR> valueMapper;

    public MapValueProcessor(Function<V, VR> valueMapper) {
        this.valueMapper = valueMapper;
    }

    @Override
    public void process(K key, V value) {
        VR newValue = valueMapper.apply(value);
        this.context().forward(key, newValue);
    }

}
