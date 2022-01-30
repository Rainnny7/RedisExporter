package me.braydon.redis.type.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;

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
     * from the given key.
     * <p>See implementations</p>
     *
     * @param jedis the jedis connection
     * @param key   the key to get the data from
     */
    @Override
    public void populateData(@NonNull Jedis jedis, @NonNull String key) {
        long length = jedis.llen(key);
        data = jedis.lrange(key, 0, length); // Get the elements from 0 to the length
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