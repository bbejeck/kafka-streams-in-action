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

package bbejeck.util.serializer;

import bbejeck.collectors.FixedSizePriorityQueue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 2/14/16
 * Time: 3:26 PM
 */

public class JsonDeserializer<T> implements Deserializer<T> {

    private Gson gson;
    private Class<T> deserializedClass;
    private Type reflectionTypeToken;

    public JsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
        init();

    }

    public JsonDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
        init();
    }

    private void init () {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(FixedSizePriorityQueue.class, new FixedSizePriorityQueueAdapter().nullSafe());
        gson = builder.create();
    }

    public JsonDeserializer() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> map, boolean b) {
        if(deserializedClass == null) {
            deserializedClass = (Class<T>) map.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
         if(bytes == null){
             return null;
         }

         Type deserializeFrom = deserializedClass != null ? deserializedClass : reflectionTypeToken;

         return gson.fromJson(new String(bytes),deserializeFrom);

    }

    @Override
    public void close() {

    }
}
