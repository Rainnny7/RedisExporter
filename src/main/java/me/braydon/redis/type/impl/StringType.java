package me.braydon.redis.type.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * The implementation of the "string" {@link KeyType}.
 *
 * @author Braydon
 */
public final class StringType extends KeyType {
    private String value;

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
        value = jedis.get(key); // Load the string
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
        pipeline.set(key, jsonElement.getAsJsonArray().get(0).getAsString()); // Save the string
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
        jsonArray.add(value);
        return jsonArray;
    }
}