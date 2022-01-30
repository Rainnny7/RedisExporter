package me.braydon.redis;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;
import lombok.NonNull;
import me.braydon.redis.common.FileUtils;
import me.braydon.redis.type.KeyType;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.Pipeline;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author Braydon
 */
public final class RedisExporter {
    private static final Gson GSON = new GsonBuilder()
            .serializeNulls()
            .setPrettyPrinting() // This isn't necessary, but it allows users to easily read their exported data
            .create();

    private static OptionSpecBuilder HELP_OPTION;

    public static void main(@NonNull String[] args) {
        // Setup the option parser
        OptionParser parser = new OptionParser() {{
            HELP_OPTION = acceptsAll(Arrays.asList("help", "h", "?"), "Show the help message");

            // The host to use during connection
            acceptsAll(Collections.singletonList("host"), "The host to connect to")
                    .withRequiredArg() // Required the argument
                    .ofType(String.class) // Use string
                    .defaultsTo("localhost"); // Default to localhost

            // The port to use during connection
            acceptsAll(Collections.singletonList("port"), "The port to connect to")
                    .withRequiredArg() // Required the argument
                    .ofType(Integer.class) // Use integer
                    .defaultsTo(6379); // Default to 6379

            // The optional password to use during connection
            acceptsAll(Collections.singletonList("password"), "The password to use during connection")
                    .withRequiredArg() // Required the argument
                    .ofType(String.class); // Use string

            // The optional database index to select during interaction
            acceptsAll(Collections.singletonList("index"), "The database index to use during connection")
                    .withRequiredArg() // Required the argument
                    .ofType(Integer.class) // Use integer
                    .defaultsTo(0);

            // Whether the user wants to export the database contents
            acceptsAll(Collections.singletonList("export"), "Whether to export or import the database")
                    .withRequiredArg() // Required the argument
                    .ofType(Boolean.class) // Use boolean
                    .defaultsTo(true); // Default to true

            // The path to the data file to use
            acceptsAll(Collections.singletonList("file"), "The data file")
                    .withRequiredArg() // Required the argument
                    .ofType(File.class) // Use file
                    .defaultsTo(new File("data.json")); // Default to data.json

            // Whether the user wants to confirm the import of the data file
            acceptsAll(Collections.singletonList("confirm"), "Whether to confirm the import");

            // Whether the user wants to flush the entire database before importing their data
            acceptsAll(Collections.singletonList("flush"), "Whether to flush the database prior to importing");
        }};
        OptionSet options = null;
        try { // Parse the arguments
            options = parser.parse(args);
        } catch (OptionException ex) {
            ex.printStackTrace();
        }
        // Display the help menu if necessary
        if (options == null || (options.has(HELP_OPTION))) {
            try {
                parser.printHelpOn(System.out);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return;
        }
        String host = (String) options.valueOf("host");
        int port = (int) options.valueOf("port");
        String password = options.has("password") ? (String) options.valueOf("password") : null;
        int index = (int) options.valueOf("index");
        boolean export = (boolean) options.valueOf("export");
        File dataFile = (File) options.valueOf("file");
        boolean confirm = options.has("confirm");
        boolean flush = options.has("flush");

        // Validate the data file
        if (dataFile.isDirectory()) { // Can only use files
            throw new IllegalArgumentException("The data file cannot be a directory");
        } else if (export && dataFile.exists()) { // Don't override data
            throw new IllegalArgumentException("The data file already exists and you are trying to export, please delete it first");
        } else if (!export && !dataFile.exists()) { // Can't import nothing
            throw new IllegalArgumentException("Cannot import a file that doesn't exist");
        } else if (!FileUtils.getFileExtension(dataFile).equalsIgnoreCase("json")) { // Can only handle json files
            throw new IllegalArgumentException("The data file must be a JSON file");
        }

        // Confirm the user wants to overwrite any existing data
        if (!export && !confirm) {
            System.err.println("WARNING: You are about to import data into the database, this will overwrite any existing data.");
            System.err.println("If you'd wish to continue, please re-run the command with the --confirm flag");
            return;
        }

        // Log the connection
        System.out.printf("Connecting to %s:%s and selecting database at index %s%n", host, port, index);

        // Setup the client config
        JedisClientConfig config = DefaultJedisClientConfig.builder()
                .password(password)
                .database(index)
                .clientName("redis-exporter")
                .build();
        try (Jedis jedis = new Jedis(host, port, config)) { // Attempt to connect
            System.out.println("Successfully connected!");
            if (export) { // Export the database
                exportDatabase(jedis, dataFile);
            } else { // Import the database
                importDatabase(jedis, flush, dataFile);
            }
        }
    }

    /**
     * Export the database to the given file.
     *
     * @param jedis the jedis connection
     * @param dataFile the data file to export to
     */
    private static void exportDatabase(@NonNull Jedis jedis, @NonNull File dataFile) {
        Set<String> keys = jedis.keys("*");
        if (keys.isEmpty()) { // If there are no keys in the database, exit
            System.out.println("No keys were found in the database, exiting...");
            return;
        }
        System.out.printf("Found %s key(s)%n", keys.size()); // Log the amount of key(s) found

        JsonObject keysObject = new JsonObject();
        long before = System.currentTimeMillis(); // Get the time before the export
        int failed = 0; // The amount of keys that failed to export
        for (String key : keys) {
            String typeName = jedis.type(key); // The name of the key type
            Class<? extends KeyType> type = KeyType.TYPES.get(typeName); // Get the type of the key
            if (type == null) { // If the key type is not supported, skip it
                failed++;
                System.err.printf("Cannot export '%s' as the type (%s) is not supported%n", key, typeName);
                continue;
            }
            try {
                KeyType keyType = type.getConstructor().newInstance(); // Constructor a new instance of the key type class
                keyType.populateFromRedis(jedis, key); // Populate the object with the data from Redis

                JsonObject keyObject = new JsonObject();
                keyObject.addProperty("type", typeName); // Add the type name to the key json object
                keyObject.addProperty("ttl", jedis.ttl(key)); // Add the time-to-live to the key json object
                keyObject.add("data", keyType.getJsonObject()); // Add the key type json object to the key json object

                keysObject.add(key, keyObject); // Add the key json object to the keys json object
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                failed++;
                ex.printStackTrace();
                continue;
            }
            System.out.printf("Exported key '%s' (%s)%n", key, typeName); // Log that the key was exported
        }
        // Save the json to the data file
        try (FileWriter writer = new FileWriter(dataFile)) {
            GSON.toJson(keysObject, writer);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        // Log that the export has finished
        System.out.printf("Export finished in %sms (success: %s, failed: %s, total: %s)%n",
                System.currentTimeMillis() - before,
                keys.size() - failed, // The amount of keys successfully exported
                failed, // The amount of keys that failed to export
                keys.size() // The total amount of keys in the database
        );
    }

    /**
     * Import the keys from the given file into the database.
     *
     * @param jedis the jedis connection
     * @param flush whether to flush the database prior to importing
     * @param dataFile the data file to import from
     */
    private static void importDatabase(@NonNull Jedis jedis, boolean flush, @NonNull File dataFile) {
        if (flush) { // If the user wants to flush the database, flush it
            long size = jedis.dbSize(); // The amount of keys in the database
            jedis.flushDB(); // Flush the database
            if (size > 0) { // If there were any key(s) flushed, log it
                System.out.printf("Flushed %s key(s)%n", size);
            }
        }
        long before = System.currentTimeMillis(); // Get the time before the import
        int failed = 0; // The amount of keys that failed to import
        int keyCount = 0; // The amount of keys in the file
        try (FileReader reader = new FileReader(dataFile)) {
            JsonObject keysObject = GSON.fromJson(reader, JsonObject.class); // Get the keys json object from the file
            Set<Map.Entry<String, JsonElement>> keys = keysObject.entrySet();
            keyCount = keys.size();

            Pipeline pipelined = jedis.pipelined(); // Create a pipeline to execute the commands in
            for (Map.Entry<String, JsonElement> entry : keys) {
                String key = entry.getKey();
                JsonObject keyObject = entry.getValue().getAsJsonObject();
                String typeName = keyObject.get("type").getAsString();
                long ttl = keyObject.get("ttl").getAsLong();
                Class<? extends KeyType> type = KeyType.TYPES.get(typeName); // Get the type of the key
                if (type == null) { // If the key type is not supported, skip it
                    failed++;
                    System.err.printf("Cannot import '%s' as the type (%s) is not supported%n", key, typeName);
                    continue;
                }
                try {
                    KeyType keyType = type.getConstructor().newInstance(); // Constructor a new instance of the key type class
                    JsonElement data = entry.getValue().getAsJsonObject().get("data");
                    keyType.saveToRedis(pipelined, key, data); // Save the key to redis
                    if (ttl > 0) { // If the key has a time to live rule, set it in Redis
                        pipelined.expire(key, ttl);
                    }
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    failed++;
                    ex.printStackTrace();
                    continue;
                }
                System.out.printf("Imported key '%s' (%s)%n", key, typeName); // Log that the key was imported
            }
            pipelined.sync(); // Execute the commands in the pipeline
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        // Log that the import has finished
        System.out.printf("Import finished in %sms (success: %s, failed: %s, total: %s)%n",
                System.currentTimeMillis() - before,
                keyCount - failed, // The amount of keys successfully imported
                failed, // The amount of keys that failed to import
                keyCount // The total amount of keys in the file
        );
    }
}