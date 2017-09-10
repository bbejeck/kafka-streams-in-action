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
import java.util.Objects;

/**
 * User: Bill Bejeck
 * Date: 2/20/16
 * Time: 9:09 AM
 */
public class Purchase {

      private String firstName;
      private String lastName;
      private String customerId;
      private String creditCardNumber;
      private String itemPurchased;
      private String department;
      private String employeeId;
      private int quantity;
      private double price;
      private Date purchaseDate;
      private String zipCode;
      private String storeId;

    private Purchase(Builder builder) {
        firstName = builder.firstName;
        lastName = builder.lastName;
        customerId = builder.customerId;
        creditCardNumber = builder.creditCardNumber;
        itemPurchased = builder.itemPurchased;
        quantity = builder.quantity;
        price = builder.price;
        purchaseDate = builder.purchaseDate;
        zipCode = builder.zipCode;
        employeeId = builder.employeeId;
        department = builder.department;
        storeId = builder.storeId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Purchase copy) {
        Builder builder = new Builder();
        builder.firstName = copy.firstName;
        builder.lastName = copy.lastName;
        builder.creditCardNumber = copy.creditCardNumber;
        builder.itemPurchased = copy.itemPurchased;
        builder.quantity = copy.quantity;
        builder.price = copy.price;
        builder.purchaseDate = copy.purchaseDate;
        builder.zipCode = copy.zipCode;
        builder.customerId = copy.customerId;
        builder.department = copy.department;
        builder.employeeId = copy.employeeId;
        builder.storeId = copy.storeId;

        return builder;
    }


    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getCreditCardNumber() {
        return creditCardNumber;
    }

    public String getItemPurchased() {
        return itemPurchased;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    public Date getPurchaseDate() {
        return purchaseDate;
    }

    public String getZipCode() {
        return zipCode;
    }

    public String getDepartment() {
        return department;
    }

    public String getEmployeeId() {
        return employeeId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getStoreId() {
        return storeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Purchase)) return false;

        Purchase purchase = (Purchase) o;

        if (quantity != purchase.quantity) return false;
        if (Double.compare(purchase.price, price) != 0) return false;
        if (firstName != null ? !firstName.equals(purchase.firstName) : purchase.firstName != null) return false;
        if (lastName != null ? !lastName.equals(purchase.lastName) : purchase.lastName != null) return false;
        if (customerId != null ? !customerId.equals(purchase.customerId) : purchase.customerId != null) return false;
        if (creditCardNumber != null ? !creditCardNumber.equals(purchase.creditCardNumber) : purchase.creditCardNumber != null)
            return false;
        if (itemPurchased != null ? !itemPurchased.equals(purchase.itemPurchased) : purchase.itemPurchased != null)
            return false;
        if (department != null ? !department.equals(purchase.department) : purchase.department != null) return false;
        if (employeeId != null ? !employeeId.equals(purchase.employeeId) : purchase.employeeId != null) return false;
        if (zipCode != null ? !zipCode.equals(purchase.zipCode) : purchase.zipCode != null) return false;
        return storeId != null ? storeId.equals(purchase.storeId) : purchase.storeId == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = firstName != null ? firstName.hashCode() : 0;
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        result = 31 * result + (customerId != null ? customerId.hashCode() : 0);
        result = 31 * result + (creditCardNumber != null ? creditCardNumber.hashCode() : 0);
        result = 31 * result + (itemPurchased != null ? itemPurchased.hashCode() : 0);
        result = 31 * result + (department != null ? department.hashCode() : 0);
        result = 31 * result + (employeeId != null ? employeeId.hashCode() : 0);
        result = 31 * result + quantity;
        temp = Double.doubleToLongBits(price);
        result = 31 * result + (int) (temp ^ (temp >>> 32));;
        result = 31 * result + (zipCode != null ? zipCode.hashCode() : 0);
        result = 31 * result + (storeId != null ? storeId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Purchase{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", customerId='" + customerId + '\'' +
                ", creditCardNumber='" + creditCardNumber + '\'' +
                ", itemPurchased='" + itemPurchased + '\'' +
                ", department='" + department + '\'' +
                ", employeeId='" + employeeId + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", purchaseDate=" + purchaseDate +
                ", zipCode='" + zipCode + '\'' +
                ", storeId='" + storeId + '\'' +
                '}';
    }

    public static final class Builder {
        private String firstName;
        private String lastName;
        private String customerId;
        private String creditCardNumber;
        private String itemPurchased;
        private int quantity;
        private double price;
        private Date purchaseDate;
        private String zipCode;
        private String department;
        private String employeeId;
        private String storeId;

        private static final String CC_NUMBER_REPLACEMENT="xxxx-xxxx-xxxx-";

        private Builder() {
        }

        public Builder firstName(String val) {
            firstName = val;
            return this;
        }

        public Builder lastName(String val) {
            lastName = val;
            return this;
        }


        public Builder maskCreditCard(){
            Objects.requireNonNull(this.creditCardNumber, "Credit Card can't be null");
            String[] parts = this.creditCardNumber.split("-");
            if (parts.length < 4 ) {
                this.creditCardNumber = "xxxx";
            } else {
                String last4Digits = this.creditCardNumber.split("-")[3];
                this.creditCardNumber = CC_NUMBER_REPLACEMENT + last4Digits;
            }
            return this;
        }

        public Builder customerId(String customerId) {
            this.customerId = customerId;
            return this;
        }

        public Builder department(String department) {
            this.department = department;
            return this;
        }

        public Builder employeeId(String employeeId) {
            this.employeeId = employeeId;
            return this;
        }

        public Builder storeId(String storeId) {
            this.storeId = storeId;
            return this;
        }

        public Builder creditCardNumber(String val) {
            creditCardNumber = val;
            return this;
        }

        public Builder itemPurchased(String val) {
            itemPurchased = val;
            return this;
        }

        public Builder quanity(int val) {
            quantity = val;
            return this;
        }

        public Builder price(double val) {
            price = val;
            return this;
        }

        public Builder purchaseDate(Date val) {
            purchaseDate = val;
            return this;
        }

        public Builder zipCode(String val) {
            zipCode = val;
            return this;
        }

        public Purchase build() {
            return new Purchase(this);
        }
    }
}
