package bbejeck.model;


import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.concurrent.ThreadLocalRandom;

public  class PublicTradedCompany {
    private double volatility;
    private double lastSold;
    private String symbol;
    private String name;
    private String sector;
    private String industry;
    private double price;
    private NumberFormat formatter = new DecimalFormat("#0.00");

    public String getSymbol() {
        return symbol;
    }

    public String getName() {
        return name;
    }

    public String getSector() {
        return sector;
    }

    public String getIndustry() {
        return industry;
    }

    public double getPrice() {
        return Double.parseDouble(formatter.format(price));
    }

    public PublicTradedCompany(double voltility, double lastSold, String symbol, String name, String sector, String industry) {
        this.volatility = volatility;
        this.lastSold = lastSold;
        this.symbol = symbol.toUpperCase();
        this.name = name;
        this.sector = sector;
        this.industry = industry;
        this.price = lastSold;
    }

    public double updateStockPrice() {
        double min = (price * -volatility);
        double max = (price * volatility);
        double randomNum = ThreadLocalRandom.current().nextDouble(min, max + 1);
        price = price + randomNum;
        return Double.parseDouble(formatter.format(price));
    }

}