package bbejeck.chapter_6.processor;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;


public class KStreamPrinter implements ProcessorSupplier {
    private String name;


    public KStreamPrinter(String name) {
        this.name = name;
    }


    @Override
    public Processor get() {
        return new PrintingProcessor(this.name);
    }


    private class PrintingProcessor extends AbstractProcessor {
        private String name;


         PrintingProcessor(String name) {
            this.name = name;
        }

        @Override
        public void process(Object key, Object value) {
            System.out.println(String.format("[%s] Key [%s] Value[%s]", name, key, value));
            this.context().forward(key, value);
        }
    }
}
