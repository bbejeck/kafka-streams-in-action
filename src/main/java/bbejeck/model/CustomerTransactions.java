package bbejeck.model;

public class CustomerTransactions {

    private String sessionInfo;
    private double totalPrice = 0;
    private long totalShares = 0;


    public CustomerTransactions update(StockTransaction stockTransaction) {
        totalShares += stockTransaction.getShares();
        totalPrice += stockTransaction.getSharePrice() * stockTransaction.getShares();

        return this;
    }

    @Override
    public String toString() {
        return "avg txn=" + totalPrice / totalShares + " sessionInfo='" + sessionInfo;
    }

    public void setSessionInfo(String sessionInfo) {
        this.sessionInfo = sessionInfo;
    }

    public CustomerTransactions merge(CustomerTransactions other) {
        this.totalShares += other.totalShares;
        this.totalPrice += other.totalPrice;
        this.sessionInfo = other.sessionInfo;
        return this;
    }
}
