package bbejeck.util.serializer;


import bbejeck.collectors.FixedSizePriorityQueue;
import bbejeck.model.CompanyStockVolume;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class FixedSizePriorityQueueAdapter extends TypeAdapter<FixedSizePriorityQueue<CompanyStockVolume>> {

    private Gson gson = new Gson();

    @Override
    public void write(JsonWriter writer, FixedSizePriorityQueue<CompanyStockVolume> value) throws IOException {

        if (value == null) {
            writer.nullValue();
            return;
        }


        Iterator<CompanyStockVolume> iterator = value.iterator();
        List<CompanyStockVolume> list = new ArrayList<>();
        while (iterator.hasNext()) {
            CompanyStockVolume stockTransaction = iterator.next();
            if (stockTransaction != null) {
                list.add(stockTransaction);
            }
        }
        writer.beginArray();
        for (CompanyStockVolume transaction : list) {
            writer.value(gson.toJson(transaction));
        }
        writer.endArray();
    }

    @Override
    public FixedSizePriorityQueue<CompanyStockVolume> read(JsonReader reader) throws IOException {
        List<CompanyStockVolume> list = new ArrayList<>();
        reader.beginArray();
        while (reader.hasNext()) {
            list.add(gson.fromJson(reader.nextString(), CompanyStockVolume.class));
        }
        reader.endArray();

        Comparator<CompanyStockVolume> c = (c1, c2) -> c2.getShares() - c1.getShares();
        FixedSizePriorityQueue<CompanyStockVolume> fixedSizePriorityQueue = new FixedSizePriorityQueue<>(c, 5);

        for (CompanyStockVolume transaction : list) {
            fixedSizePriorityQueue.add(transaction);
        }

        return fixedSizePriorityQueue;
    }
}
