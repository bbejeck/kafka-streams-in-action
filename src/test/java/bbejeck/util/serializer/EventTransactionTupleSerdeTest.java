package bbejeck.util.serializer;


import bbejeck.model.DayTradingAppClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class EventTransactionTupleSerdeTest {

    private StockTransaction transaction;
    private DayTradingAppClickEvent dayTradingAppClickEvent;
    private Tuple<List<DayTradingAppClickEvent>, List<StockTransaction>> eventTuple;

    private Serde<Tuple<List<DayTradingAppClickEvent>, List<StockTransaction>>> tupleSerde = StreamsSerdes.EventTransactionTupleSerde();

    @Before
    public void setUp() {

        transaction = StockTransaction.newBuilder()
                .withCustomerId("custId")
                .withIndustry("foo")
                .withPurchase(false)
                .withSector("sector")
                .withSharePrice(25.25)
                .withShares(500)
                .withSymbol("XYZ").build();

        dayTradingAppClickEvent = new DayTradingAppClickEvent("XYZ", "http://link", Instant.now());
        List<DayTradingAppClickEvent> eventList = new ArrayList<>();
        List<StockTransaction> transactionList = new ArrayList<>();

        eventList.add(dayTradingAppClickEvent);
        transactionList.add(transaction);

        eventTuple = Tuple.of(eventList, transactionList);
    }


    @Test
    public void testSerializeDeserialize() throws Exception {

        byte[] bytes = tupleSerde.serializer().serialize("topic", eventTuple);

        Tuple<List<DayTradingAppClickEvent>, List<StockTransaction>> deserializedTuple = tupleSerde.deserializer().deserialize("topic", bytes);

        List<DayTradingAppClickEvent> deserializedEvts = deserializedTuple._1;
        List<StockTransaction> deserializedTxns = deserializedTuple._2;

        assertThat(deserializedEvts.size(), is(1));
        assertThat(deserializedTxns.size(), is(1));
        assertEquals(deserializedEvts.get(0), dayTradingAppClickEvent);
        assertEquals(deserializedTxns.get(0), transaction);
    }

}
