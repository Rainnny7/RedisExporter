package me.braydon.redis.type.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.Set;

/**
 * The implementation of the "hash" {@link KeyType}.
 *
 * @author Braydon
 */
public final class SetType extends KeyType {
    private Set<String> data = new HashSet<>();

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
        data = jedis.smembers(key);
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