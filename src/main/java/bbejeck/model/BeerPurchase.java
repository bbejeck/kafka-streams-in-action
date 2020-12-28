package bbejeck.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BeerPurchase {

  private Currency currency;
  private double totalSale;
  private int numberCases;
  private String beerType;

  @Override
  public String toString() {
    return "BeerPurchase{"
        + "currency="
        + currency
        + ", totalSale="
        + totalSale
        + ", numberCases="
        + numberCases
        + ", beerType='"
        + beerType
        + '\''
        + '}';
  }
}
