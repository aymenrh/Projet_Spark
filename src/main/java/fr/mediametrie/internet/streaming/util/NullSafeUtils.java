package fr.mediametrie.internet.streaming.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Useful functions to work with null.
 */
public class NullSafeUtils {

    /**
     * @return the given value if not null or the default value otherwise.
     */
    public static String getStringOrDefault(String value, String defaultValue) {
        return value == null ? defaultValue : value;
    }


    /**
     * @return the first non null value from the given list or null if all the values are null or arg is null.
     */
    public static <T> T firstNonNull(T... values) {
        if (values == null) {
            return null;
        }
        Optional<T> first = Arrays.stream(values).filter(Objects::nonNull).findFirst();
        return first.orElse(null);
    }

    /**
     * @return the uppercase value of the given string or null if the given string is null.
     */
    public static String toUpperCase(String value) {
        return value == null ? null : value.toUpperCase();
    }
}
