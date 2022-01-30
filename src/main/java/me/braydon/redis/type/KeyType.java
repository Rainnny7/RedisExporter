package me.braydon.redis.type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.impl.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Collections;
import java.util.Map;

/**
 * @author Braydon
 */
public abstract class KeyType {
    /**
     * A map of all key types.
     * <p>
     * The key is the type identifier
     * and the value is the implementation
     * for the type.
     * </p>
     */
    public static final Map<String, Class<? extends KeyType>> TYPES = Collections.synchronizedMap(Map.of(
            "string", StringType.class,
            "list", ListType.class,
            "set", SetType.class,
            "zset", SortedSetType.class,
            "hash", HashType.class
    ));

    /**
     * Populate this object with the data
     * from the given key in Redis.
     * <p>See implementations</p>
     *
     * @param jedis the jedis connection
     * @param key the key to get the data from
     * @see Jedis for jedis
     */
    public abstract void populateFromRedis(@NonNull Jedis jedis, @NonNull String key);

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
    public abstract void saveToRedis(@NonNull Pipeline pipeline, @NonNull String key, @NonNull JsonElement jsonElement);

    /**
     * Get the json object representation of
     * this object.
     *
     * @return the json object
     * @see JsonObject for json object
     */
    @NonNull
    public abstract JsonElement getJsonObject();
}