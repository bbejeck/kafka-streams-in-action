package bbejeck.util.datagen;

import bbejeck.model.Purchase;
import com.github.javafaker.Faker;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class DataGenerator {

    public static void main(String[] args) {
        Faker faker = new Faker();
        Gson gson = new Gson();
        LocalDateTime localDateTime = LocalDateTime.now();
        for (int i = 0; i < 10; i++) {
//            Address address = faker.address();
//            Name name = faker.name();
//            System.out.println(address.streetAddressNumber()+"  "+address.streetName()+" "+address.streetSuffix()+", "+address.city()+"  "+address.state()+" "+address.zipCode());
//            System.out.println(name.fullName());

            String jsonDate = gson.toJson(localDateTime);
            LocalDateTime dateTime = gson.fromJson(jsonDate, LocalDateTime.class);
            System.out.println(jsonDate);
            System.out.println(dateTime);
            System.out.println(Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant()));
            localDateTime = localDateTime.plusMinutes(3).plusSeconds(23);

        }

    }

    //TODO generate people, address customer ids
    //TODO generate products, Faker commerce, coffee drinks, electronic purchases   product id's for each one  dollar amounts
    //TODO store zip codes store id's
    //TODO dates for all purchases
    //TODO collection of customers for joins with coffee and electronics
    //TODO generate dates in different orders to show out order data handling
    //TODO store generated data as json is specified formats orders to show different scenarios


    public static List<Purchase> generatePurchases(int number) {

        LocalDateTime localDateTime = LocalDateTime.of(2017, Month.FEBRUARY, 20, 13, 20,15,234);
        List<Purchase> purchases = new ArrayList<>();

        Purchase purchase = Purchase.builder().purchaseDate(convertLocalDateTimeToDate(localDateTime)).creditCardNumber("1111-2222-3333-4444").customerId("3333").department("coffee").itemPurchased("mocha").price(5.00).build();
        purchases.add(purchase);

        Date date = convertLocalDateTimeToDate(localDateTime.plusMinutes(90));
        purchases.add(Purchase.builder(purchase).department("electronics").itemPurchased("laptop").price(2000.00).purchaseDate(date).build());

        return purchases;

    }

    private static Date convertLocalDateTimeToDate(LocalDateTime localDateTime) {
        Preconditions.checkNotNull(localDateTime, "LocalDateTime object can't be null");
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static Purchase generatePurchase() {
        return generatePurchases(1).get(0);
    }


    private static List<Date> getPurchaseDates(int numberDates, int withinTime){
        
       throw new IllegalStateException("Not Implemented");
    }
}
