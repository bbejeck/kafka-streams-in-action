package bbejeck.webserver;


import bbejeck.model.StockPerformance;
import bbejeck.model.StockTransaction;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static spark.Spark.get;
import static spark.Spark.port;


public class InteractiveQueryServer {

    private static final Logger LOG = LoggerFactory.getLogger(InteractiveQueryServer.class);
    private final Gson gson = new Gson();
    private final KafkaStreams kafkaStreams;
    private Client client = ClientBuilder.newClient();
    private final HostInfo hostInfo;
    private static final String STORE_PARAM = ":store";
    private static final String KEY_PARAM = ":key";
    private static final String FROM_PARAM = ":from";
    private static final String TO_PARAM = ":to";
    private StringSerializer stringSerializer = new StringSerializer();
    private volatile boolean ready = false;
    private static final String STORES_NOT_ACCESSIBLE = "{\"message\":\"Stores not ready for service, probably re-balancing\"}";


    public InteractiveQueryServer(final KafkaStreams kafkaStreams, final HostInfo hostInfo) {
        this.kafkaStreams = kafkaStreams;
        this.hostInfo = hostInfo;
        port(hostInfo.port());
    }

    public void init() {
        LOG.info("Started the Interactive Query Web server");
        get("/window/:store/:key/:from/:to", (req, res) -> ready ? fetchFromWindowStore(req.params()) : STORES_NOT_ACCESSIBLE);
        get("/window/:store/:key", (req, res) -> ready ? fetchFromWindowStore(req.params()) : STORES_NOT_ACCESSIBLE);
        get("/kv/:store", (req, res) ->  ready ? fetchAllFromKeyValueStore(req.params()) : STORES_NOT_ACCESSIBLE);
        get("/session/:store/:key", (req, res) -> ready ? fetchFromSessionStore(req.params()) : STORES_NOT_ACCESSIBLE);

    }

    private String fetchAllFromKeyValueStore(Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        ReadOnlyKeyValueStore<String, StockPerformance> readOnlyKeyValueStore = kafkaStreams.store(store, QueryableStoreTypes.keyValueStore());
        List<KeyValue<String, StockPerformance>> keyValues = new ArrayList<>();
        try(KeyValueIterator<String,StockPerformance> iterator = readOnlyKeyValueStore.all()) {
               while(iterator.hasNext()) {
                   keyValues.add(iterator.next());
               }
        }
       return gson.toJson(keyValues);
    }

    private String fetchFromSessionStore(Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        String key = params.get(KEY_PARAM);

        HostInfo storeHostInfo = getHostInfo(store, key);

        if(storeHostInfo.host().equals("unknown")){
            return STORES_NOT_ACCESSIBLE;
        }
        
        if(dataNotLocal(storeHostInfo)){
            LOG.info("{} located in state store on another instance", key);
            return fetchRemote(storeHostInfo,"session", params);
        }

        ReadOnlySessionStore<String, StockTransaction> readOnlySessionStore = kafkaStreams.store(store,QueryableStoreTypes.sessionStore());
        List<StockTransaction> results = new ArrayList<>();
        try(KeyValueIterator<Windowed<String>, StockTransaction> iterator = readOnlySessionStore.fetch(key)){
            while(iterator.hasNext()) {
                results.add(iterator.next().value);
            }
        }
        return gson.toJson(results);
    }

    private String fetchFromWindowStore(Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        String key = params.get(KEY_PARAM);
        String fromStr = params.get(FROM_PARAM);
        String toStr = params.get(TO_PARAM);

        HostInfo storeHostInfo = getHostInfo(store, key);

        if(storeHostInfo.host().equals("unknown")){
            return STORES_NOT_ACCESSIBLE;
        }
        
        if(dataNotLocal(storeHostInfo)) {
            LOG.info("{} located in state store in another instance", key);
           return fetchRemote(storeHostInfo,"window", params);
        }


        Instant instant = Instant.now();
        long now = instant.toEpochMilli();
        long from =  fromStr != null ? Long.parseLong(fromStr) : now - 60000;
        long to =  toStr != null ? Long.parseLong(toStr) : now;
        List<Integer> results = new ArrayList<>();

        ReadOnlyWindowStore<String, Integer> readOnlyWindowStore = kafkaStreams.store(store, QueryableStoreTypes.windowStore());
        try(WindowStoreIterator<Integer> iterator = readOnlyWindowStore.fetch(key, from , to)){
             while (iterator.hasNext()) {
                 results.add(iterator.next().value);
             }
        }
        return gson.toJson(results);
    }

    private String fetchRemote(HostInfo hostInfo, String path,Map<String, String> params) {
        String store = params.get(STORE_PARAM);
        String key = params.get(KEY_PARAM);
        String from = params.get(FROM_PARAM);
        String to = params.get(TO_PARAM);

        String url;
        
        if (from != null && to !=null) {
           url =  String.format("http://%s:%d/%s/%s/%s/%s/%s", hostInfo.host(), hostInfo.port(), path, store, key,from,to);
        }else{
           url = String.format("http://%s:%d/%s/%s/%s", hostInfo.host(), hostInfo.port(), path, store, key);
        }
        return client.target(url).request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    }
    
    private boolean dataNotLocal(HostInfo hostInfo) {
        return !this.hostInfo.equals(hostInfo);
    }
    
    private HostInfo getHostInfo(String storeName, String key) {
         StreamsMetadata metadata = kafkaStreams.metadataForKey(storeName, key, stringSerializer);
         return metadata.hostInfo();
    }

    public synchronized void setReady(boolean ready) {
        this.ready = ready;
    }

    public void stop() {
        Spark.stop();
        client.close();
        LOG.info("Shutting down the Interactive Query Web server");
    }
    
}
