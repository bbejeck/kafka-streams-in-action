package bbejeck.chapter_6.processor;

import bbejeck.model.BeerPurchase;
import bbejeck.model.Currency;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.text.DecimalFormat;

public class BeerPurchaseProcessor extends AbstractProcessor<String, BeerPurchase> {

  private static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###.##");

  private String domesticSalesNode;         // the node can be a processor or a sink.
  private String internationalSalesNode;    // the node can be a processor or a sink.

  public BeerPurchaseProcessor(String domesticSalesNode, String internationalSalesNode) {
    this.domesticSalesNode = domesticSalesNode;
    this.internationalSalesNode = internationalSalesNode;
  }

  @Override
  public void process(String key, BeerPurchase beerPurchase) {

    Currency transactionCurrency = beerPurchase.getCurrency();
    if (transactionCurrency != Currency.DOLLARS) {
      BeerPurchase dollarBeerPurchase = this.convertToDollarPurchase(beerPurchase);
      context().forward(key, dollarBeerPurchase, internationalSalesNode);
    } else {
      context().forward(key, beerPurchase, domesticSalesNode);
    }
  }

  private BeerPurchase convertToDollarPurchase(BeerPurchase beerPurchase) {

    double totalSaleInDollar = beerPurchase.getCurrency().convertToDollars(beerPurchase.getTotalSale());
    double formattedTotalSaleInDollar = Double.parseDouble(DECIMAL_FORMAT.format(totalSaleInDollar));

    return BeerPurchase.builder()
        .numberCases(beerPurchase.getNumberCases())
        .beerType(beerPurchase.getBeerType())
        .currency(Currency.DOLLARS)
        .totalSale(formattedTotalSaleInDollar)
        .build();
  }
}
