/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bbejeck.model;

/**
 * User: Bill Bejeck
 * Date: 2/20/16
 * Time: 9:55 AM
 */
public class RewardAccumulator {

    private String customerId;
    private double purchaseTotal;
    private int totalRewardPoints;
    private int currentRewardPoints;
    private int daysFromLastPurchase;

    private RewardAccumulator(String customerId, double purchaseTotal, int rewardPoints) {
        this.customerId = customerId;
        this.purchaseTotal = purchaseTotal;
        this.currentRewardPoints = rewardPoints;
        this.totalRewardPoints = rewardPoints;
    }

    public String getCustomerId() {
        return customerId;
    }

    public double getPurchaseTotal() {
        return purchaseTotal;
    }

    public int getCurrentRewardPoints() {
        return currentRewardPoints;
    }

    public int getTotalRewardPoints() {
        return totalRewardPoints;
    }

    public void addRewardPoints(int previousTotalPoints) {
        this.totalRewardPoints += previousTotalPoints;
    }

    @Override
    public String toString() {
        return "RewardAccumulator{" +
                "customerId='" + customerId + '\'' +
                ", purchaseTotal=" + purchaseTotal +
                ", totalRewardPoints=" + totalRewardPoints +
                ", currentRewardPoints=" + currentRewardPoints +
                ", daysFromLastPurchase=" + daysFromLastPurchase +
                '}';
    }

    public static Builder builder(Purchase purchase){return new Builder(purchase);}

    public static final class Builder {
        private String customerId;
        private double purchaseTotal;
        private int daysSinceLastVisit;
        private int rewardPoints;

        private Builder(Purchase purchase){
           this.customerId = purchase.getLastName()+","+purchase.getFirstName();
           this.purchaseTotal = purchase.getPrice() * (double) purchase.getQuantity();
           this.rewardPoints = (int) purchaseTotal;
        }


        public RewardAccumulator build(){
            return new RewardAccumulator(customerId, purchaseTotal, rewardPoints);
        }

    }
}
