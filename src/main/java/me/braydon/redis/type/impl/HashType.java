package me.braydon.redis.type.impl;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;

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
     * from the given key.
     * <p>See implementations</p>
     *
     * @param jedis the jedis connection
     * @param key   the key to get the data from
     */
    @Override
    public void populateData(@NonNull Jedis jedis, @NonNull String key) {
        data = jedis.hgetAll(key);
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