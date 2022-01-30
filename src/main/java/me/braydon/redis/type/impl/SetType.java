package me.braydon.redis.type.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashSet;
import java.util.Set;

/**
 * The implementation of the "set" {@link KeyType}.
 *
 * @author Braydon
 */
public final class SetType extends KeyType {
    private Set<String> data = new HashSet<>();

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
        data = jedis.smembers(key);
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
        pipeline.sadd(key, data.toArray(new String[0])); // Save the set
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