package bbejeck.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * User: Bill Bejeck
 * Date: 11/29/17
 * Time: 10:42 PM
 */
public class CustomerTransactions {

    private Map<String,Integer> stockPurchasedAmounts = new HashMap<>();
    private String sessionInfo;


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
                ", sessionInfo='" + sessionInfo + '\'' +
                '}';
    }

    public void setSessionInfo(String sessionInfo) {
        this.sessionInfo = sessionInfo;
    }

    public CustomerTransactions merge(CustomerTransactions other) {
        Set<String> keys = new HashSet<>();
        Map<String, Integer> merged = new HashMap<>();
        keys.addAll(other.stockPurchasedAmounts.keySet());
        keys.addAll(this.stockPurchasedAmounts.keySet());
        for (String key : keys) {
             Integer count = this.stockPurchasedAmounts.getOrDefault(key, 0);
             Integer countOther = other.stockPurchasedAmounts.getOrDefault(key, 0);
             merged.put(key, count + countOther);

        }
        this.stockPurchasedAmounts = merged;
        return this;
    }
}
