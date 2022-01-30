package me.braydon.redis.type.impl;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.*;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.HashSet;
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
     * from the given key.
     * <p>See implementations</p>
     *
     * @param jedis the jedis connection
     * @param key   the key to get the data from
     */
    @Override
    public void populateData(@NonNull Jedis jedis, @NonNull String key) {
        Set<SortedSetEntry> data = new HashSet<>();
        for (Tuple entry : jedis.zrangeByScoreWithScores(key, "-inf", "+inf")) {
            data.add(new SortedSetEntry(entry.getElement(), entry.getScore()));
        }
        this.data = data;
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