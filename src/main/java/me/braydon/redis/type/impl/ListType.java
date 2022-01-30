package me.braydon.redis.type.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * The implementation of the "list" {@link KeyType}.
 *
 * @author Braydon
 */
public final class ListType extends KeyType {
    private List<String> data = new ArrayList<>();

    /**
     * Populate this object with the data
     * from the given key in Redis.
     * <p>See implementations</p>
     *
     * @param jedis the jedis connection
     * @param key the key to get the data from
     * @see Jedis for jedis
     */
    @Override
    public void populateFromRedis(@NonNull Jedis jedis, @NonNull String key) {
        long length = jedis.llen(key); // The length of the list
        data = jedis.lrange(key, 0, length); // Get the elements from 0 to the length
    }

    /**
     * Save the data in the given json element
     * to Redis.
     *
     * @param pipeline the pipelined jedis connection
     * @param key the key to save the data to
     * @param jsonElement the json element containing the data
     * @see Pipeline for pipeline
     * @see JsonElement for the json element
     */
    @Override
    public void saveToRedis(@NonNull Pipeline pipeline, @NonNull String key, @NonNull JsonElement jsonElement) {
        for (JsonElement element : jsonElement.getAsJsonArray()) {
            data.add(element.getAsString());
        }
        pipeline.lpush(key, data.toArray(new String[0])); // Save the list
    }

    /**
     * Get the json object representation of
     * this object.
     *
     * @return the json object
     * @see JsonObject for json object
     */
    @Override @NonNull
    public JsonElement getJsonObject() {
        JsonArray jsonArray = new JsonArray();
        for (String entry : data) { // Add all the entries to the json object
            jsonArray.add(entry);
        }
        return jsonArray;
    }
}