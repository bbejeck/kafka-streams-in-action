package bbejeck.model;


import java.text.NumberFormat;

public class ShareVolume {

    private String symbol;
    private int shares;
    private String industry;


    private ShareVolume(Builder builder) {
        symbol = builder.symbol;
        shares = builder.shares;
        industry = builder.industry;
    }


    public String getIndustry() {
        return industry;
    }

    public String getSymbol() {
        return symbol;
    }

    public int getShares() {
        return shares;
    }


    @Override
    public String toString() {
        NumberFormat numberFormat = NumberFormat.getInstance();
        return "ShareVolume{" +
                "symbol='" + symbol + '\'' +
                ", shares=" + numberFormat.format(shares) +
                '}';
    }

    public static ShareVolume sum(ShareVolume csv1, ShareVolume csv2) {
        Builder builder = newBuilder(csv1);
        builder.shares = csv1.shares + csv2.shares;
        return builder.build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(StockTransaction stockTransaction) {
        Builder builder = new Builder();
        builder.symbol = stockTransaction.getSymbol();
        builder.shares = stockTransaction.getShares();
        builder.industry = stockTransaction.getIndustry();
        return builder;
    }

    public static Builder newBuilder(ShareVolume copy) {
        Builder builder = new Builder();
        builder.symbol = copy.symbol;
        builder.shares = copy.shares;
        builder.industry = copy.industry;
        return builder;
    }


    public static final class Builder {
        private String symbol;
        private int shares;
        private String industry;

        private Builder() {
        }

        public Builder withSymbol(String val) {
            symbol = val;
            return this;
        }

        public Builder withShares(int val) {
            shares = val;
            return this;
        }

        public Builder withIndustry(String val) {
            industry = val;
            return this;
        }

        public ShareVolume build() {
            return new ShareVolume(this);
        }
    }
}
