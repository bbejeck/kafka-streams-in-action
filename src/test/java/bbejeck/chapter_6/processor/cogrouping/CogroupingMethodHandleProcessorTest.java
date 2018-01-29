package bbejeck.chapter_6.processor.cogrouping;

import bbejeck.MockKeyValueStore;
import bbejeck.model.ClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static bbejeck.chapter_6.processor.cogrouping.CogroupingMethodHandleProcessor.TUPLE_STORE_NAME;
import static org.apache.kafka.streams.processor.PunctuationType.STREAM_TIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

public class CogroupingMethodHandleProcessorTest {

    private ProcessorContext processorContext = mock(ProcessorContext.class);
    private CogroupingMethodHandleProcessor processor = new CogroupingMethodHandleProcessor();
    private MockKeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> keyValueStore = new MockKeyValueStore<>();
    private ClickEvent clickEvent = new ClickEvent("ABC", "http://somelink.com", Instant.now());
    private StockTransaction transaction = StockTransaction.newBuilder().withSymbol("ABC").build();

    @Test
    @DisplayName("Processor should initialize correctly")
    public void testInitializeCorrectly() {
        processor.init(processorContext);
        verify(processorContext).schedule(eq(15000L), eq(STREAM_TIME), isA(Punctuator.class));
        verify(processorContext).getStateStore(TUPLE_STORE_NAME);
    }

    @Test
    @DisplayName("Process method should store results")
    public void testProcessCorrectly() {

        when(processorContext.getStateStore(TUPLE_STORE_NAME)).thenReturn(keyValueStore);

        processor.init(processorContext);

        processor.process("ABC", Tuple.of(clickEvent, null));

        Tuple<List<ClickEvent>,List<StockTransaction>> tuple = keyValueStore.innerStore().get("ABC");

        assertThat(tuple._1.get(0), equalTo(clickEvent));
        assertThat(tuple._2.isEmpty(), equalTo(true));

        processor.process("ABC", Tuple.of(null, transaction));

        assertThat(tuple._1.get(0), equalTo(clickEvent));
        assertThat(tuple._2.get(0), equalTo(transaction));

        assertThat(tuple._1.size(), equalTo(1));
        assertThat(tuple._2.size(), equalTo(1));

    }

    @Test
    @DisplayName("Punctuate should forward records")
    public void testPunctuateProcess(){
        when(processorContext.getStateStore(TUPLE_STORE_NAME)).thenReturn(keyValueStore);
        
        processor.init(processorContext);
        processor.process("ABC", Tuple.of(clickEvent, null));
        processor.process("ABC", Tuple.of(null, transaction));

        Tuple<List<ClickEvent>,List<StockTransaction>> tuple = keyValueStore.innerStore().get("ABC");
        List<ClickEvent> clickEvents = new ArrayList<>(tuple._1);
        List<StockTransaction> stockTransactions = new ArrayList<>(tuple._2);

        processor.cogroup(124722348947L);

        verify(processorContext).forward("ABC", Tuple.of(clickEvents, stockTransactions));

        assertThat(tuple._1.size(), equalTo(0));
        assertThat(tuple._2.size(), equalTo(0));
    }

}