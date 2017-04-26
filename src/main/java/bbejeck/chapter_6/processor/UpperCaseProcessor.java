package bbejeck.chapter_6.processor;


import org.apache.kafka.streams.processor.AbstractProcessor;

public class UpperCaseProcessor extends AbstractProcessor<String, String> {


    @Override
    public void process(String key, String value) {

        context().forward(key, value.toUpperCase());

    }
}
