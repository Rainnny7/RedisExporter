package me.braydon.redis.type.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;

/**
 * The implementation of the "string" {@link KeyType}.
 *
 * @author Braydon
 */
public final class StringType extends KeyType {
    private String value;

    /**
     * Populate this object with the data
     * from the given key.
     * <p>See implementations</p>
     *
     * @param jedis the jedis connection
     * @param key   the key to get the data from
     */
    @Override
    public void populateData(@NonNull Jedis jedis, @NonNull String key) {
        value = jedis.get(key);
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