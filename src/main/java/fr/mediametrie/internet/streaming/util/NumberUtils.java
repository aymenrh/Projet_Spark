package fr.mediametrie.internet.streaming.util;

import java.util.Optional;
import java.util.Random;

/**
 * Provide useful methods related to numbers.
 */
public class NumberUtils {

    /**
     * @return the long value parsed from the given String or {@link Optional#empty()} if the given
     *         value is null or not a number.
     */
    public static Optional<Long> parseLong(String s) {
        try {
            return Optional.ofNullable(Long.parseLong(s));
        } catch (NullPointerException | NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * Round the given value using the given step.
     * <pre>
     *     roundDown(13000L, 2000) = 12000L
     *     roundDown(12000L, 2000) = 12000L
     *     roundDown(12001L, 2000) = 12000L
     *     roundDown(13999L, 2000) = 12000L
     * </pre>
     */
    public static long roundDown(long value, int step) {
        return value - (value % step);
    }

    /**
     * Round the given value using the given step. If the value is on a step, the previous step is returned.
     * <pre>
     *     roundDown(13000L, 2000) = 12000L
     *     roundDown(12000L, 2000) = 10000L
     *     roundDown(12001L, 2000) = 12000L
     *     roundDown(13999L, 2000) = 12000L
     * </pre>
     */
    public static long roundDownExclude(long value, int step) {
        long mod = value % step;
        return mod == 0 ? value - step : value - mod;
    }
}
