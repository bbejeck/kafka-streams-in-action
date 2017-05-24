package bbejeck.model;


public enum Currency {

    EURO(1.09),

    POUNDS(1.2929),

    DOLLARS(1.0);


    Currency(double conversionRate) {
         this.conversionRate = conversionRate;
    }

    private double conversionRate;


    public double convertToDollars(double internationalAmount) {
          return internationalAmount/conversionRate;
    }
}
