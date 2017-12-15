package bbejeck.chapter_9;


import bbejeck.clients.producer.MockDataProducer;

public class StockPerformanceInteractiveQueryDataProducer {

    public static void main(String[] args) {
        MockDataProducer.produceStockTransactionsForIQ();
        Runtime.getRuntime().addShutdownHook(new Thread(MockDataProducer::shutdown));
    }
}
