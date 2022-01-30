package me.braydon.redis.type.impl;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Map;

/**
 * The implementation of the "hash" {@link KeyType}.
 *
 * @author Braydon
 */
public final class HashType extends KeyType {
    private Map<String, String> data = new HashMap<>();

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
        data = jedis.hgetAll(key);
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
        for (Map.Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
            data.put(entry.getKey(), entry.getValue().getAsString());
        }
        pipeline.hmset(key, data); // Save the map
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
        JsonObject jsonObject = new JsonObject();
        for (Map.Entry<String, String> entry : data.entrySet()) { // Add all the entries to the json object
            jsonObject.addProperty(entry.getKey(), entry.getValue());
        }
        return jsonObject;
    }
}