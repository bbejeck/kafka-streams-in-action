package bbejeck.model;


import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayDeque;

public class StockPerformance {

    private Instant lastUpdateSent;
    private static final int MAX_LOOK_BACK = 20;
    private double currentPrice = 0.0;
    private double priceDifferential = 0.0;
    private double shareDifferential = 0.0;
    private int currentShareVolume = 0;
    private double currentAveragePrice = Double.MIN_VALUE;
    private double currentAverageVolume = Double.MIN_VALUE;
    private ArrayDeque<Double> shareVolumeLookback = new ArrayDeque<>(MAX_LOOK_BACK);
    private ArrayDeque<Double> sharePriceLookback = new ArrayDeque<>(MAX_LOOK_BACK);
    private transient DecimalFormat decimalFormat = new DecimalFormat("#.00");
    


    public void setLastUpdateSent(Instant lastUpdateSent) {
        this.lastUpdateSent = lastUpdateSent;
    }

    public void updatePriceStats(double currentPrice) {
        this.currentPrice = currentPrice;
        priceDifferential = calculateDifferentialFromAverage(currentPrice, currentAveragePrice);
        currentAveragePrice = calculateNewAverage(currentPrice, currentAveragePrice, sharePriceLookback);
    }

    public void updateVolumeStats(int currentShareVolume) {
        this.currentShareVolume = currentShareVolume;
        shareDifferential = calculateDifferentialFromAverage((double) currentShareVolume, currentAverageVolume);
        currentAverageVolume = calculateNewAverage(currentShareVolume, currentAverageVolume, shareVolumeLookback);
    }

    private double calculateDifferentialFromAverage(double value, double average) {
        return average != Double.MIN_VALUE ? ((value / average) - 1) * 100.0 : 0.0;
    }

    private double calculateNewAverage(double newValue, double currentAverage, ArrayDeque<Double> deque) {
        if (deque.size() < MAX_LOOK_BACK) {
            deque.add(newValue);

            if (deque.size() == MAX_LOOK_BACK) {
                currentAverage = deque.stream().reduce(0.0, Double::sum) / MAX_LOOK_BACK;
            }
            
        } else {
            double oldestValue = deque.poll();
            deque.add(newValue);
            currentAverage = (currentAverage + (newValue / MAX_LOOK_BACK)) - oldestValue / MAX_LOOK_BACK;
        }
        return currentAverage;
    }

    public double priceDifferential() {
        return priceDifferential;
    }

    public double volumeDifferential() {
        return shareDifferential;
    }

    public double getCurrentPrice() {
        return currentPrice;
    }

    public int getCurrentShareVolume() {
        return currentShareVolume;
    }

    public double getCurrentAveragePrice() {
        return currentAveragePrice;
    }

    public double getCurrentAverageVolume() {
        return currentAverageVolume;
    }

    public Instant getLastUpdateSent() {
        return lastUpdateSent;
    }

    @Override
    public String toString() {
        return "StockPerformance{" +
                "lastUpdateSent= " + lastUpdateSent +
                ", currentPrice= " + decimalFormat.format(currentPrice) +
                ", currentAveragePrice= " + decimalFormat.format(currentAveragePrice) +
                ", percentage difference= " + decimalFormat.format(priceDifferential) +
                ", currentShareVolume= " + decimalFormat.format(currentShareVolume) +
                ", currentAverageVolume= " + decimalFormat.format(currentAverageVolume) +
                ", shareDifferential= " + decimalFormat.format(shareDifferential) +
                '}';
    }
}
