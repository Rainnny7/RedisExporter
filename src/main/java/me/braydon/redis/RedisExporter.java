package me.braydon.redis;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
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
                importDatabase(jedis, dataFile);
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
                keyType.populateData(jedis, key); // Populate the object with the data from Redis

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
            System.err.printf("Exported key '%s' (%s)%n", key, typeName); // Log that the key was exported
        }
        // Save the json to the data file
        try (FileWriter writer = new FileWriter(dataFile)) {
            GSON.toJson(keysObject, writer);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // Log that the export has finished
        System.err.printf("Export finished in %sms (success: %s, failed: %s, total: %s)%n",
                System.currentTimeMillis() - before,
                keys.size() - failed,
                failed,
                keys.size()
        );
    }

    private static void importDatabase(@NonNull Jedis jedis, @NonNull File dataFile) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}