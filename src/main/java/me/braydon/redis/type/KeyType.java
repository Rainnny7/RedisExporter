package me.braydon.redis.type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.NonNull;
import me.braydon.redis.type.impl.HashType;
import me.braydon.redis.type.impl.SetType;
import redis.clients.jedis.Jedis;

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
            "hash", HashType.class,
            "set", SetType.class
    ));

    /**
     * Populate this object with the data
     * from the given key.
     * <p>See implementations</p>
     *
     * @param jedis the jedis connection
     * @param key the key to get the data from
     */
    public abstract void populateData(@NonNull Jedis jedis, @NonNull String key);

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