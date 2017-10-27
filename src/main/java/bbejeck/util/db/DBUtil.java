package bbejeck.util.db;


import bbejeck.model.StockTransaction;
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
import java.util.concurrent.CountDownLatch;

public class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    private static Server h2Server;
    private static final String DB_URL = "jdbc:h2:tcp://localhost:9989/~/findata";
    private static final String USER = "sa";
    private static final String PW = "";
    private static  Connection connection;
    private static String insertStatement = "insert into transactions(SYMBOL, SECTOR, INDUSTRY,SHARES, SHAREPRICE, CUSTOMERID, TRANSACTIONTIMESTAMP) values(?,?,?,?,?,?,?)";
    private static int lastId = 0;

    private static String createStatement = "create table if not exists transactions(TXN_ID bigint auto_increment," +
            "SYMBOL varchar(255), SECTOR varchar(255), INDUSTRY varchar(255), " +
            "SHARES integer, SHAREPRICE double, CUSTOMERID varchar(255), TRANSACTIONTIMESTAMP TIMESTAMP ) ";

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

    public static void insertData( List<StockTransaction> data) {
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
        ResultSet rs = statement.executeQuery("select * from transactions where \"txn_id\" =" + lastId);
        while (rs.next()) {
            Integer txn_id = rs.getInt("txn_id");
            String sybmol = rs.getString("symbol");
            String sector = rs.getString("sector");
            String industry = rs.getString("industry");
            Integer shares = rs.getInt("shares");
            Double sharePrice = rs.getDouble("sharePrice");
            String customerId = rs.getString("customerId");
            Date date = rs.getTimestamp("transactionTimestamp");

            System.out.println(txn_id + " "+ sybmol+" "+sector+" "+industry+" "+shares+" "+sharePrice+" "+customerId+" "+ date);
        }

    }


    public static void main(String[] args) throws Exception {

        if(args.length > 0 && args[0].equalsIgnoreCase("insert")) {
            LOG.info("inserting data");
            DBUtil.initConnection();
            StockTransaction transaction = StockTransaction.newBuilder().withSymbol("XYZ")
                    .withShares(100)
                    .withIndustry("Industry")
                    .withSector("Sector")
                    .withSharePrice(25.53D)
                    .withCustomerId("customer")
                    .withTransactionTimestamp(new Date()).build();

            DBUtil.insertData(Collections.singletonList(transaction));
            DBUtil.printTest();
            DBUtil.closeConnection();

        } else{
            LOG.info("Database server started");
            DBUtil.startDatabaseServer();

            CountDownLatch countDownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(countDownLatch::countDown));
            countDownLatch.await();
            DBUtil.stopDatabaseServer();
        }
    }
    


    

}
