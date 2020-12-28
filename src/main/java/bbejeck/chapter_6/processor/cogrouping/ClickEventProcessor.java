package bbejeck.chapter_6.processor.cogrouping;

import bbejeck.model.ClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ClickEventProcessor extends AbstractProcessor<String, ClickEvent> {

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    super.init(context);
  }

  @Override
  public void process(String key, ClickEvent clickEvent) {
    if (key != null) {
      Tuple<ClickEvent, StockTransaction> tuple = Tuple.of(clickEvent, null);
      context().forward(key, tuple);
    }
  }
}
