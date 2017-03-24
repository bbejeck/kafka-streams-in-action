package bbejeck.model;

/**
 * User: Bill Bejeck
 * Date: 3/21/17
 * Time: 11:04 PM
 */
public class TransactionCount {

    long count;
    String symbol;

    private TransactionCount(Builder builder) {
        count = builder.count;
        symbol = builder.symbol;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public long getCount() {
        return count;
    }

    public String getSymbol() {
        return symbol;
    }


    public static final class Builder {
        private long count;
        private String symbol;

        private Builder() {
        }

        public Builder withCount(long val) {
            count = val;
            return this;
        }

        public Builder withSymbol(String val) {
            symbol = val;
            return this;
        }
        

        public TransactionCount build() {
            return new TransactionCount(this);
        }
    }
}
