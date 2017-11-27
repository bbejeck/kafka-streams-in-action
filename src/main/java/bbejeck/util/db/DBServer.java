package bbejeck.util.db;


import bbejeck.model.PublicTradedCompany;
import bbejeck.model.StockTransaction;
import bbejeck.util.datagen.DataGenerator;
import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DBServer {
    private static final Logger LOG = LoggerFactory.getLogger(DBServer.class);

    private static Server h2Server;
    private static final String DB_URL = "jdbc:h2:tcp://localhost:9989/~/findata";
    private static final String USER = "sa";
    private static final String PW = "";
    private static  Connection connection;
    private static String insertStatement = "insert into transactions(SMBL, SCTR, INDSTY, SHRS, SHRPRC, CSTMRID, TXNTS) values(?,?,?,?,?,?,?)";
    private static int lastId = 0;
    private static ExecutorService executorService = Executors.newCachedThreadPool();

    private static String createStatement = "create table if not exists transactions(TXN_ID bigint auto_increment," +
            "SMBL varchar(255), SCTR varchar(255), INDSTY varchar(255), " +
            "SHRS integer, SHRPRC double, CSTMRID varchar(255), TXNTS TIMESTAMP ) ";

    public static void startDatabaseServer() throws SQLException {
        h2Server = Server.createTcpServer("-tcp", "-tcpPort", "9989", "-tcpAllowOthers");
        h2Server.start();
    }

    public static void stopDatabaseServer() {
        if (h2Server != null) {
            h2Server.stop();
        }
    }

    public static void initConnection() {
        try {
            Class.forName("org.h2.Driver");
            connection = DriverManager.getConnection(DB_URL);
            connection.setAutoCommit(false);
            connection.prepareStatement(createStatement).execute();
        } catch (ClassNotFoundException | SQLException e) {
            LOG.error("Problem opening connection", e);
        }
    }

    public static Future<Integer> startDataInserts(int numberIterations) {

        Runnable serverThread = () -> {
            try {
                startDatabaseServer();
                LOG.info("Database server started");
            }catch (SQLException e) {
                LOG.error("Error starting sql server", e);
            }
        };

        executorService.submit(serverThread);

        Callable<Integer> insertThread = () -> {
            LOG.info("Starting the insert thread with {} iterations", numberIterations);
            initConnection();
            LOG.info("established db connection");
            int numberTransactions = 50;

            List<DataGenerator.Customer> customers = DataGenerator.generateCustomers(25);
            List<PublicTradedCompany> companies = DataGenerator.generatePublicTradedCompanies(100);

            for (int i = 0; i < numberIterations; i++) {
                List<StockTransaction> stockTransactions = DataGenerator.generateStockTransactions(customers, companies, numberTransactions);
                insertData(stockTransactions);
                Thread.sleep(10000);
                LOG.info("Inserted a batch to database");
            }
            return numberIterations * numberTransactions;
        };

        LOG.info("Starting the inserts");
        return executorService.submit(insertThread);
    }

    private static void insertData(List<StockTransaction> data) {
            PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(insertStatement,Statement.RETURN_GENERATED_KEYS);
            for (StockTransaction transaction : data) {
                 statement.setString(1,transaction.getSymbol());
                 statement.setString(2,transaction.getSector());
                 statement.setString(3,transaction.getIndustry());
                 statement.setInt(4,transaction.getShares());
                 statement.setDouble(5,transaction.getSharePrice());
                 statement.setString(6,transaction.getCustomerId());
                 statement.setTimestamp(7, new java.sql.Timestamp(transaction.getTransactionTimestamp().getTime()));

                 statement.addBatch();
            }
            statement.executeBatch();
            ResultSet resultSet = statement.getGeneratedKeys();
            if(resultSet.next()){
                lastId = resultSet.getInt(1);
                LOG.info("Last generated ID "+lastId);
            }
            connection.commit();
        } catch (SQLException e) {
            LOG.error("Problem executing insert", e);
        } finally {
            try {
                if(statement!= null) {
                    statement.close();
                }
            } catch (SQLException e) {
                LOG.error("Problems closing statement", e);
            }
        }
    }

    public static void shutdown() {
        closeConnection();
        stopDatabaseServer();
        executorService.shutdownNow();
    }

    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.error("Problems closing database", e);
            }
        }
    }

    public static void printTest() throws Exception {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from transactions where \"TXN_ID\" =" + lastId);
        while (rs.next()) {
            Integer txn_id = rs.getInt("txn_id");
            String sybmol = rs.getString("smbl");
            String sector = rs.getString("sctr");
            String industry = rs.getString("indsty");
            Integer shares = rs.getInt("shrs");
            Double sharePrice = rs.getDouble("shrprc");
            String customerId = rs.getString("cstmrid");
            Date date = rs.getTimestamp("txnts");

            System.out.println(txn_id + " "+ sybmol+" "+sector+" "+industry+" "+shares+" "+sharePrice+" "+customerId+" "+ date);
        }

    }


    public static void main(String[] args) throws Exception {

        if (args.length > 0 && args[0].equalsIgnoreCase("test")) {
            LOG.info("inserting data");
            DBServer.initConnection();
            StockTransaction transaction = StockTransaction.newBuilder().withSymbol("XYZ")
                    .withShares(100)
                    .withIndustry("Industry")
                    .withSector("Sector")
                    .withSharePrice(25.53D)
                    .withCustomerId("customer")
                    .withTransactionTimestamp(new Date()).build();

            DBServer.insertData(Collections.singletonList(transaction));
            DBServer.printTest();
            DBServer.closeConnection();
        } else {
            LOG.info("Kicking off db server and inserting data");
            Future<Integer> totalInsertCounts = startDataInserts(200);
            Runtime.getRuntime().addShutdownHook(new Thread(DBServer::shutdown));
            totalInsertCounts.get();
            LOG.info("All inserts completed, shutting down now");
            shutdown();
        }
    }
    


    

}
