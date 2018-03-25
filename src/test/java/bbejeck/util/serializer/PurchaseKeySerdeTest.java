package bbejeck.util.serializer;

import bbejeck.model.PurchaseKey;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static org.mockito.internal.matchers.Equality.areEqual;


/**
 * User: Bill Bejeck
 * Date: 3/25/18
 * Time: 6:50 PM
 */
public class PurchaseKeySerdeTest {

    private PurchaseKey purchaseKey;
    private Serde<PurchaseKey> purchaseKeySerde = StreamsSerdes.purchaseKeySerde();

    @Before
    public void setUp() {
        purchaseKey = new PurchaseKey("123345", new Date());
    }


    @Test
    public void testSerializePurchaseKey() {
        byte[] serialized = purchaseKeySerde.serializer().serialize("topic", purchaseKey);
        PurchaseKey deserialized = purchaseKeySerde.deserializer().deserialize("topic", serialized);
        areEqual(purchaseKey, deserialized);

    }
}
