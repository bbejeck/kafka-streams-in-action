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

import java.util.Date;


public class StockTransaction {

    private String symbol;
    private String sector;
    private String industry;
    private int shares;
    private double sharePrice;
    private String customerId;
    private Date transactionTimestamp;
    private boolean purchase;

    private StockTransaction(Builder builder) {
        symbol = builder.symbol;
        sector = builder.sector;
        industry = builder.industry;
        shares = builder.shares;
        sharePrice = builder.sharePrice;
        customerId = builder.customerId;
        transactionTimestamp = builder.transactionTimestamp;
        purchase = builder.purchase;
    }


    public static StockTransaction reduce(StockTransaction transactionOne, StockTransaction transactionTwo){
        StockTransaction.Builder transactionBuilder = StockTransaction.newBuilder(transactionOne);
        transactionBuilder.withShares(transactionOne.getShares() + transactionTwo.getShares());

        return transactionBuilder.build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(StockTransaction copy) {
        Builder builder = new Builder();
        builder.symbol = copy.symbol;
        builder.sector = copy.sector;
        builder.industry = copy.industry;
        builder.shares = copy.shares;
        builder.sharePrice = copy.sharePrice;
        builder.customerId = copy.customerId;
        builder.transactionTimestamp = copy.transactionTimestamp;
        builder.purchase = copy.purchase;
        return builder;
    }


    public String getSymbol() {
        return symbol;
    }


    public String getSector() {
        return sector;
    }


    public int getShares() {
        return shares;
    }


    public double getSharePrice() {
        return sharePrice;
    }


    public Date getTransactionTimestamp() {
        return transactionTimestamp;
    }

    public String getCustomerId() {
        return customerId;
    }


    public String getIndustry() {
        return industry;
    }


    public boolean isPurchase() {
        return purchase;
    }



    @Override
    public String toString() {
        return "StockTransaction{" +
                "symbol='" + symbol + '\'' +
                ", sector='" + sector + '\'' +
                ", shares=" + shares +
                ", sharePrice=" + sharePrice +
                ", customerId='" + customerId + '\'' +
                ", transactionTimestamp=" + transactionTimestamp +
                ", purchase=" + purchase +
                '}';
    }


    public static final class Builder {
        private String symbol;
        private String sector;
        private String industry;
        private int shares;
        private double sharePrice;
        private String customerId;
        private Date transactionTimestamp;
        private boolean purchase;

        private Builder() {
        }

        public Builder withSymbol(String val) {
            symbol = val;
            return this;
        }

        public Builder withSector(String val) {
            sector = val;
            return this;
        }

        public Builder withIndustry(String val) {
            industry = val;
            return this;
        }

        public Builder withShares(int val) {
            shares = val;
            return this;
        }

        public Builder withSharePrice(double val) {
            sharePrice = val;
            return this;
        }

        public Builder withCustomerId(String val) {
            customerId = val;
            return this;
        }

        public Builder withTransactionTimestamp(Date val) {
            transactionTimestamp = val;
            return this;
        }

        public Builder withPurchase(boolean val) {
            purchase = val;
            return this;
        }

        public StockTransaction build() {
            return new StockTransaction(this);
        }
    }
}
