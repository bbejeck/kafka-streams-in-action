package bbejeck.util.datagen;

import bbejeck.model.PublicTradedCompany;
import bbejeck.model.Purchase;
import bbejeck.model.StockTransaction;
import com.github.javafaker.ChuckNorris;
import com.github.javafaker.Faker;
import com.github.javafaker.Finance;
import com.github.javafaker.Name;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;


public class DataGenerator {

    public static final int NUMBER_UNIQUE_CUSTOMERS = 100;
    public static final int NUMBER_UNIQUE_STORES = 15;
    public static final int NUMBER_TEXT_STATEMENTS = 15;
    public static final int DEFAULT_NUM_PURCHASES = 100;
    public static final int NUMBER_TRADED_COMPANIES = 50;
    public static final int NUM_ITERATIONS = 10;

    private static Faker dateFaker = new Faker();
    private static Supplier<Date> timestampGenerator = () -> dateFaker.date().past(12, TimeUnit.HOURS, new Date());

    private DataGenerator() {
    }



    public static void setTimestampGenerator(Supplier<Date> timestampGenerator) {
        DataGenerator.timestampGenerator = timestampGenerator;
    }


    public static List<String> generateRandomText() {
        List<String> phrases = new ArrayList<>(NUMBER_TEXT_STATEMENTS);
        Faker faker = new Faker();

        for (int i = 0; i < NUMBER_TEXT_STATEMENTS; i++) {
            ChuckNorris chuckNorris = faker.chuckNorris();
            phrases.add(chuckNorris.fact());
        }
        return phrases;
    }

    public static List<String> generateFinancialNews() {
        List<String> news = new ArrayList<>(9);
        Faker faker = new Faker();
        for (int i = 0; i < 9; i++) {
            news.add(faker.company().bs());
        }
        return news;
    }

    public static List<Purchase> generatePurchases(int number, int numberCustomers) {
        List<Purchase> purchases = new ArrayList<>();

        Faker faker = new Faker();
        List<Customer> customers = generateCustomers(numberCustomers);
        List<Store> stores = generateStores();

        Random random = new Random();
        for (int i = 0; i < number; i++) {
            String itemPurchased = faker.commerce().productName();
            int quantity = faker.number().numberBetween(1, 5);
            double price = Double.parseDouble(faker.commerce().price(4.00, 295.00));
            Date purchaseDate = timestampGenerator.get();

            Customer customer = customers.get(random.nextInt(numberCustomers));
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

    public static List<StockTransaction> generateStockTransactions(List<Customer> customers, List<PublicTradedCompany> companies, int number) {
        List<StockTransaction> transactions = new ArrayList<>(number);
        Faker faker = new Faker();
        for (int i = 0; i < number; i++) {
            int numberShares = faker.number().numberBetween(100, 50000);
            Customer customer = customers.get(faker.number().numberBetween(0, customers.size()));
            PublicTradedCompany company = companies.get(faker.number().numberBetween(0, companies.size()));
            Date transactionDate = timestampGenerator.get();
            StockTransaction transaction = StockTransaction.newBuilder().withCustomerId(customer.customerId).withTransactionTimestamp(transactionDate)
                    .withIndustry(company.getIndustry()).withSector(company.getSector()).withSharePrice(company.updateStockPrice()).withShares(numberShares)
                    .withSymbol(company.getSymbol()).withPurchase(true).build();
            transactions.add(transaction);
        }
        return transactions;
    }


    public static List<PublicTradedCompany> stockTicker(int numberCompanies) {
        return generatePublicTradedCompanies(numberCompanies);
    }


    public static List<PublicTradedCompany> generatePublicTradedCompanies(int numberCompanies) {
        List<PublicTradedCompany> companies = new ArrayList<>();
        Faker faker = new Faker();
        Random random = new Random();
        for (int i = 0; i < numberCompanies; i++) {
            String name = faker.company().name();
            String stripped = name.replaceAll("[^A-Za-z]", "");
            int start = random.nextInt(stripped.length() - 4);
            String symbol = stripped.substring(start, start + 4);
            double volatility = Double.parseDouble(faker.options().option("0.01", "0.02", "0.03", "0.04", "0.05", "0.06", "0.07", "0.08", "0.09"));
            double lastSold = faker.number().randomDouble(2, 15, 150);
            String sector = faker.options().option("Energy", "Finance", "Technology", "Transportation", "Health Care");
            String industry = faker.options().option("Oil & Gas Production", "Coal Mining", "Commercial Banks", "Finance/Investors Services", "Computer Communications Equipment", "Software Consulting", "Aerospace", "Railroads", "Major Pharmaceuticals");
            companies.add(new PublicTradedCompany(volatility, lastSold, symbol, name, sector, industry));
        }

        return companies;

    }


    private static Purchase generateCafePurchase(Purchase purchase, Faker faker) {
        Date date = purchase.getPurchaseDate();
        Instant adjusted = date.toInstant().minus(faker.number().numberBetween(5, 18), ChronoUnit.MINUTES);
        Date cafeDate = Date.from(adjusted);

        return Purchase.builder(purchase).department("Coffee")
                .itemPurchased(faker.options().option("Mocha", "Mild Roast", "Red-Eye", "Dark Roast"))
                .price(Double.parseDouble(faker.commerce().price(3.00, 6.00))).quanity(1).purchaseDate(cafeDate).build();

    }

    public static List<Customer> generateCustomers(int numberCustomers) {
        List<Customer> customers = new ArrayList<>(numberCustomers);
        Faker faker = new Faker();
        List<String> creditCards = generateCreditCardNumbers(numberCustomers);
        for (int i = 0; i < numberCustomers; i++) {
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


    public static class Customer {
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

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public String getCustomerId() {
            return customerId;
        }

        public String getCreditCardNumber() {
            return creditCardNumber;
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
