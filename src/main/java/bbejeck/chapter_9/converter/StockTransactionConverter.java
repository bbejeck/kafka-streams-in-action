package bbejeck.chapter_9.converter;


import com.google.gson.JsonObject;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;


public class StockTransactionConverter implements Converter {

    private static final Logger LOG = LoggerFactory.getLogger(StockTransactionConverter.class);
    private Map<String, String> columnMappings;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

        columnMappings = new HashMap<>();
        columnMappings.put("CUSTOMERID", "customerId");
        columnMappings.put("SHAREPRICE", "sharePrice");
        columnMappings.put("TRANSACTIONTIMESTAMP", "transactionTimestamp");
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {

        Struct struct = (Struct) value;
        JsonObject jsonObject = new JsonObject();


        for (Field field : schema.fields()) {
            String fieldName = field.name();
            String objName = columnMappings.get(fieldName);

            objName = objName == null ? fieldName.toLowerCase() : objName;
            String objVal = struct.get(field.name()).toString();
            if(objName.equals("transactionTimestamp")){
                objVal = objVal.replace(' ', 'T') + "-04:00";
                LOG.info("Updated date {}", objVal);
            }
            jsonObject.addProperty(objName, objVal);

        }
        LOG.info("Converted to json {}", jsonObject);

        return jsonObject.toString().getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return null;
    }
}
