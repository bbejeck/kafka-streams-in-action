package bbejeck.model;


public class StockTickerData {

    private double price;
    private  String symbol;

    public double getPrice() {
        return price;
    }

    public String getSymbol() {
        return symbol;
    }

    public StockTickerData(double price, String symbol) {
        this.price = price;
        this.symbol = symbol;
    }

    @Override
    public String toString() {
        return "StockTickerData{" +
                "price=" + price +
                ", symbol='" + symbol + '\'' +
                '}';
    }
}
