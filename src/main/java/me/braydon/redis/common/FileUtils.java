package me.braydon.redis.common;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.io.File;

/**
 * @author Braydon
 */
@UtilityClass
public final class FileUtils {
    /**
     * Get the extension of the given file.
     *
     * @param file the file
     * @return the file extension
     * @see File for file
     */
    public static String getFileExtension(@NonNull File file) {
        String fileName = file.getName();
        int dotIndex = fileName.lastIndexOf('.');
        return dotIndex == -1 ? "N/A" : fileName.substring(dotIndex + 1);
    }
}