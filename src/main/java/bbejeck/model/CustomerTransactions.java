package bbejeck.model;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 11/29/17
 * Time: 10:42 PM
 */
public class CustomerTransactions {

    private Map<String,Integer> stockPurchasedAmounts = new HashMap<>();


    public CustomerTransactions update(StockTransaction stockTransaction) {
          int currentShares = stockTransaction.getShares();
          String symbol = stockTransaction.getSymbol();

          Integer numberShares = stockPurchasedAmounts.get(symbol);
          if (numberShares == null) {
              numberShares = 0;
          }

          numberShares+= currentShares;
          stockPurchasedAmounts.put(symbol, numberShares);
       return this;
    }

    @Override
    public String toString() {
        return "CustomerTransactions{" +
                "stockPurchasedAmounts=" + stockPurchasedAmounts +
                '}';
    }

    public CustomerTransactions merge(CustomerTransactions other) {
           throw new IllegalStateException("Implement me");
    }
}
