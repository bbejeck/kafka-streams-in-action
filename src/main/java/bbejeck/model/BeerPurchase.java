package bbejeck.model;


public class BeerPurchase {

    private Currency currency;
    private double totalSale;
    private int numberCases;
    private String beerType;

    private BeerPurchase(Builder builder) {
        currency = builder.currency;
        totalSale = builder.totalSale;
        numberCases = builder.numberCases;
        beerType = builder.beerType;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(BeerPurchase copy) {
        Builder builder = new Builder();
        builder.currency = copy.currency;
        builder.totalSale = copy.totalSale;
        builder.numberCases = copy.numberCases;
        builder.beerType = copy.beerType;
        return builder;
    }


    public Currency getCurrency() {
        return currency;
    }

    public double getTotalSale() {
        return totalSale;
    }

    public int getNumberCases() {
        return numberCases;
    }

    public String getBeerType() {
        return beerType;
    }

    @Override
    public String toString() {
        return "BeerPurchase{" +
                "currency=" + currency +
                ", totalSale=" + totalSale +
                ", numberCases=" + numberCases +
                ", beerType='" + beerType + '\'' +
                '}';
    }


    public static final class Builder {
        private Currency currency;
        private double totalSale;
        private int numberCases;
        private String beerType;

        private Builder() {
        }

        public Builder currency(Currency val) {
            currency = val;
            return this;
        }

        public Builder totalSale(double val) {
            totalSale = val;
            return this;
        }

        public Builder numberCases(int val) {
            numberCases = val;
            return this;
        }

        public Builder beerType(String val) {
            beerType = val;
            return this;
        }

        public BeerPurchase build() {
            return new BeerPurchase(this);
        }
    }
}
