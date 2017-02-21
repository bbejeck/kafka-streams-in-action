package bbejeck.chapter_4.joiner;

import bbejeck.model.CorrelatedPurchase;
import bbejeck.model.Purchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Arrays;
import java.util.Date;
import java.util.List;


public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {

    @Override
    public CorrelatedPurchase apply(Purchase purchase, Purchase otherPurchase) {

        CorrelatedPurchase.Builder builder = CorrelatedPurchase.newBuilder();

        Date otherPurchaseDate = otherPurchase != null ? otherPurchase.getPurchaseDate() : null;
        Double otherPrice = otherPurchase != null ? otherPurchase.getPrice() : 0.0;
        String otherItemPurchased = otherPurchase != null ? otherPurchase.getItemPurchased() : null;

        List<String> purchasedItems = Arrays.asList(purchase.getItemPurchased());

        if (otherItemPurchased != null) {
            purchasedItems.add(otherItemPurchased);
        }


        builder.withCustomerId(purchase.getCustomerId())
                .withFirstPurchaseDate(purchase.getPurchaseDate())
                .withSecondPurchaseDate(otherPurchaseDate)
                .withItemsPurchased(purchasedItems)
                .withTotalAmount(purchase.getPrice() + otherPrice);

        return builder.build();
    }
}
