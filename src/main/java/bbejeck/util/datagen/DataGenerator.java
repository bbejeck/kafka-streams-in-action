package bbejeck.util.datagen;

import bbejeck.model.Purchase;
import com.github.javafaker.Beer;
import com.github.javafaker.ChuckNorris;
import com.github.javafaker.Faker;
import com.github.javafaker.Finance;
import com.github.javafaker.Name;
import com.github.javafaker.Shakespeare;
import com.github.javafaker.Superhero;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


public class DataGenerator {

    private static final int NUMBER_UNIQUE_CUSTOMERS = 100;
    private static final int NUMBER_UNIQUE_STORES = 15;
    private static final int NUMBER_TEXT_STATEMENTS = 15;

    private DataGenerator(){}



    public static List<String> generateRandomText() {
        List<String> phrases  = new ArrayList<>(NUMBER_TEXT_STATEMENTS);
        Faker faker = new Faker();

        for (int i = 0; i < NUMBER_TEXT_STATEMENTS; i++) {
            ChuckNorris chuckNorris = faker.chuckNorris();
            phrases.add(chuckNorris.fact());
        }
       return phrases;
    }

    public static List<Purchase> generatePurchases(int number) {
        List<Purchase> purchases = new ArrayList<>();

        Faker faker = new Faker();
        List<Customer> customers = generateCustomers();
        List<Store> stores = generateStores();

        Random random = new Random();
        for (int i = 0; i < number; i++) {
            String itemPurchased = faker.commerce().productName();
            int quantity = faker.number().numberBetween(1, 5);
            double price = Double.parseDouble(faker.commerce().price(4.00, 15.00));
            Date purchaseDate = faker.date().past(12, TimeUnit.HOURS, new Date());

            Customer customer = customers.get(random.nextInt(NUMBER_UNIQUE_CUSTOMERS));
            Store store = stores.get(random.nextInt(NUMBER_UNIQUE_STORES));

            Purchase purchase = Purchase.builder().creditCardNumber(customer.creditCardNumber).customerId(customer.customerId)
                    .department(store.department).employeeId(store.employeeId).firstName(customer.firstName)
                    .lastName(customer.lastName).itemPurchased(itemPurchased).quanity(quantity).price(price).purchaseDate(purchaseDate)
                    .zipCode(store.zipCode).storeId(store.storeId).build();


            if (purchase.getDepartment().toLowerCase().contains("electronics")) {
                Purchase cafePurchase = generateCafePurchase(purchase, faker);
                purchases.add(cafePurchase);
            }
          purchases.add(purchase);
        }

        return purchases;

    }

    private static Purchase generateCafePurchase(Purchase purchase, Faker faker) {
        Date date = purchase.getPurchaseDate();
        Instant adjusted = date.toInstant().minus(faker.number().numberBetween(5, 18), ChronoUnit.MINUTES);
        Date cafeDate = Date.from(adjusted);

        return Purchase.builder(purchase).department("Coffee")
                .itemPurchased(faker.options().option("Mocha", "Mild Roast", "Red-Eye", "Dark Roast"))
                .price(Double.parseDouble(faker.commerce().price(3.00, 6.00))).quanity(1).purchaseDate(cafeDate).build();

    }

    private static List<Customer> generateCustomers() {
        List<Customer> customers = new ArrayList<>(NUMBER_UNIQUE_CUSTOMERS);
        Faker faker = new Faker();
        List<String> creditCards = generateCreditCardNumbers(NUMBER_UNIQUE_CUSTOMERS);
        for (int i = 0; i < NUMBER_UNIQUE_CUSTOMERS; i++) {
            Name name = faker.name();
            String creditCard = creditCards.get(i);
            String customerId = faker.idNumber().valid();
            customers.add(new Customer(name.firstName(), name.lastName(), customerId, creditCard));
        }
        return customers;
    }

    private static List<String> generateCreditCardNumbers(int numberCards) {
        int counter = 0;
        Pattern visaMasterCardAmex = Pattern.compile("(\\d{4}-){3}\\d{4}");
        List<String> creditCardNumbers = new ArrayList<>(numberCards);
        Finance finance = new Faker().finance();
        while (counter < numberCards) {
            String cardNumber = finance.creditCard();
            if (visaMasterCardAmex.matcher(cardNumber).matches()) {
                creditCardNumbers.add(cardNumber);
                counter++;
            }
        }
        return creditCardNumbers;
    }

    private static List<Store> generateStores() {
        List<Store> stores = new ArrayList<>(NUMBER_UNIQUE_STORES);
        Faker faker = new Faker();
        for (int i = 0; i < NUMBER_UNIQUE_STORES; i++) {
            String department = (i % 5 == 0) ? "Electronics" : faker.commerce().department();
            String employeeId = Long.toString(faker.number().randomNumber(5, false));
            String zipCode = faker.options().option("47197-9482", "97666", "113469", "334457");
            String storeId = Long.toString(faker.number().randomNumber(6, true));
            if (i + 1 == NUMBER_UNIQUE_STORES) {
                employeeId = "000000"; //Seeding id for employee security check
            }
            stores.add(new Store(employeeId, zipCode, storeId, department));
        }

        return stores;
    }


    private static class Customer {
        private String firstName;
        private String lastName;
        private String customerId;
        private String creditCardNumber;

        private Customer(String firstName, String lastName, String customerId, String creditCardNumber) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.customerId = customerId;
            this.creditCardNumber = creditCardNumber;
        }
    }

    private static class Store {
        private String employeeId;
        private String zipCode;
        private String storeId;
        private String department;

        private Store(String employeeId, String zipCode, String storeId, String department) {
            this.employeeId = employeeId;
            this.zipCode = zipCode;
            this.storeId = storeId;
            this.department = department;
        }
    }
}
