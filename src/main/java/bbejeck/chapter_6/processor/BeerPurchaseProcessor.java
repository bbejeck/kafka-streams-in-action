package bbejeck.chapter_6.processor;

import bbejeck.model.BeerPurchase;
import bbejeck.model.Currency;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.text.DecimalFormat;

import static bbejeck.model.Currency.DOLLARS;


public class BeerPurchaseProcessor extends AbstractProcessor<String, BeerPurchase> {

    private String domesticSalesNode;
    private String internationalSalesNode;

    public BeerPurchaseProcessor(String domesticSalesNode, String internationalSalesNode) {
        this.domesticSalesNode = domesticSalesNode;
        this.internationalSalesNode = internationalSalesNode;
    }

    @Override
    public void process(String key, BeerPurchase beerPurchase) {

        Currency transactionCurrency = beerPurchase.getCurrency();
        if (transactionCurrency != DOLLARS) {
            BeerPurchase.Builder builder = BeerPurchase.newBuilder(beerPurchase);
            double internationalSaleAmount = beerPurchase.getTotalSale();
            String pattern = "###.##";
            DecimalFormat decimalFormat = new DecimalFormat(pattern);
            builder.totalSale(Double.parseDouble(decimalFormat.format(transactionCurrency.convertToDollars(internationalSaleAmount))));
            beerPurchase = builder.build();
            context().forward(key, beerPurchase, internationalSalesNode);
        } else {
            context().forward(key, beerPurchase, domesticSalesNode);
        }

    }
}
