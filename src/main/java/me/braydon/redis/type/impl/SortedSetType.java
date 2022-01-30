package me.braydon.redis.type.impl;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.*;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.resps.Tuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The implementation of the "zset" {@link KeyType}.
 *
 * @author Braydon
 */
public final class SortedSetType extends KeyType {
    private Set<SortedSetEntry> data = new HashSet<>();

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
        Set<SortedSetEntry> data = new HashSet<>();
        for (Tuple entry : jedis.zrangeByScoreWithScores(key, "-inf", "+inf")) {
            data.add(new SortedSetEntry(entry.getElement(), entry.getScore()));
        }
        this.data = data;
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
        Map<String, Double> entries = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
            entries.put(entry.getKey(), entry.getValue().getAsDouble());
        }
        pipeline.zadd(key, entries); // Save the zset
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
        for (SortedSetEntry entry : data) { // Add all the entries to the json object
            jsonObject.addProperty(entry.getKey(), entry.getScore());
        }
        return jsonObject;
    }

    @AllArgsConstructor @Getter @EqualsAndHashCode(onlyExplicitlyIncluded = true) @ToString
    public static class SortedSetEntry {
        /**
         * The key of the entry.
         */
        @EqualsAndHashCode.Include private final String key;

        /**
         * The score of the entry.
         */
        private final double score;
    }
}