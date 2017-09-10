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

/**
 * User: Bill Bejeck
 * Date: 2/21/16
 * Time: 3:36 PM
 */
public class PurchasePattern {

    private String zipCode;
    private String item;
    private Date date;
    private double amount;


    private PurchasePattern(Builder builder) {
        zipCode = builder.zipCode;
        item = builder.item;
        date = builder.date;
        amount = builder.amount;

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder builder(Purchase purchase){
        return new Builder(purchase);

    }
    public String getZipCode() {
        return zipCode;
    }

    public String getItem() {
        return item;
    }

    public Date getDate() {
        return date;
    }

    public double getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return "PurchasePattern{" +
                "zipCode='" + zipCode + '\'' +
                ", item='" + item + '\'' +
                ", date=" + date +
                ", amount=" + amount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PurchasePattern)) return false;

        PurchasePattern that = (PurchasePattern) o;

        if (Double.compare(that.amount, amount) != 0) return false;
        if (zipCode != null ? !zipCode.equals(that.zipCode) : that.zipCode != null) return false;
        return item != null ? item.equals(that.item) : that.item == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = zipCode != null ? zipCode.hashCode() : 0;
        result = 31 * result + (item != null ? item.hashCode() : 0);
        temp = Double.doubleToLongBits(amount);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public static final class Builder {
        private String zipCode;
        private String item;
        private Date date;
        private double amount;

        private  Builder() {
        }

        private Builder(Purchase purchase) {
            this.zipCode = purchase.getZipCode();
            this.item = purchase.getItemPurchased();
            this.date = purchase.getPurchaseDate();
            this.amount = purchase.getPrice() * purchase.getQuantity();
        }

        public Builder zipCode(String val) {
            zipCode = val;
            return this;
        }

        public Builder item(String val) {
            item = val;
            return this;
        }

        public Builder date(Date val) {
            date = val;
            return this;
        }

        public Builder amount(double amount) {
            this.amount = amount;
            return this;
        }

        public PurchasePattern build() {
            return new PurchasePattern(this);
        }
    }
}
