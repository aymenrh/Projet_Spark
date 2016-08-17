package fr.mediametrie.internet.streaming.hit;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import fr.mediametrie.internet.streaming.util.DateTimeUtils;
import fr.mediametrie.internet.streaming.util.NullSafeUtils;

/**
 * Useful methods to work with hits...
 */
public class HitUtils {

    public static final HashFunction hashFunction = Hashing.murmur3_128();

    /**
     * Retrieve the timestamp from the given map representing the "qs" subobject in a json hit.
     * The timestamp is retrieved from "qs.ts".
     *
     * @return the long value in millis of the ts field or 0 if no match or invalid number.
     */
    public static long parseTimestampFromQs(Map<String, String> qs) {
        try {
            return Long.parseLong(qs.getOrDefault(Hit.JSON_QS_TS, "0")) * 1000; // qs.ts is in seconds.
        } catch (NullPointerException | NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Return the parsed int value from the given string or the default value if input is null, empty or not an int.
     */
    public static int parseIntOrDefault(String value, int defaultValue) {

        try {
            Double valueDouble = Double.valueOf(value);
            /* If String value is "NaN" or "Infinity", Double.valueOf return a Double type NaN or Infinity.
             * But in this case we wan't to return 0 */
            if (Double.isNaN(valueDouble) || (Double.isInfinite(valueDouble)) ) {
                return 0;
            } else {
                return (int) Math.floor(valueDouble);
            }
        } catch (NullPointerException | NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Return the parsed long value from the given string or the default value if input is null, empty or not an long.
     */
    public static Long parseLongOrDefault(String value, Long defaultValue) {
        try {
            return Long.valueOf(value);
        } catch (NullPointerException | NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Return the date associated to a hit in the french timezone
     *
     * @param hit The hit to the get the date of
     * @return The date of the hit
     */
    public static LocalDate parseHitDate(Hit hit) {
        Optional<Instant> ts = hit.getHitTimestamp();
        return ts.isPresent() ? LocalDateTime.ofInstant(ts.get(), DateTimeUtils.ZONE_ID).toLocalDate()
                : null;
    }

    public static String dateFromEpochMillis(long ts, @Nonnull DateTimeFormatter formatter) {
        return formatter.format(Instant.ofEpochMilli(ts));
    }

    public static String dateFromEpochSeconds(long ts, @Nonnull DateTimeFormatter formatter) {
        return formatter.format(Instant.ofEpochSecond(ts));
    }

    public static String dateFromEpochMillis(long ts) {
        return DateTimeUtils.INTERNATIONAL_DATE.format(Instant.ofEpochMilli(ts));
    }

    public static String dateFromEpochSeconds(long ts) {
        return DateTimeUtils.INTERNATIONAL_DATE.format(Instant.ofEpochSecond(ts));
    }

    /**
     * @return if the given value for a multilevel fields is relevant (not empty and not "-").
     */
    public static boolean isMultiLevelFieldRelevant(String value) {
        return value != null && value.length() > 0 && !"-".equals(value);
    }

    /**
     * @return if the given value for a cookie is relevant (not empty and not "-").
     */
    public static boolean isCookieRelevant(String value) {
        return value != null && value.length() > 0 && !"-".equals(value);
    }

    /**
     * Parse the given hit date using the default hit format and returns it formatetd using the given formatter.
     */
    public static String formatHitDate(String date, DateTimeFormatter formatter) {
        LocalDate ld = LocalDate.parse(date, DateTimeUtils.INTERNATIONAL_DATE);
        return formatter.format(ld);
    }

    /**
     * @return the given value if not null or empty or "-" otherwise.
     */
    public static String dashIfNullOrEmpty(String value) {
        return Strings.isNullOrEmpty(value) ? "-" : value;
    }

    /**
     * Extract the first Long found in the given input.
     * 
     * <pre>
     *     null -> null
     *     "" -> null
     *     "bla" -> null
     *     "12345.6789" -> 12345
     *     "foo123456789bar" -> 123456789
     * </pre>
     */
    public static Long extractTimestamp(String value) {
        Long ts = null;
        if (!Strings.isNullOrEmpty(value)) {
            Scanner scanner = new Scanner(value);
            if (scanner.useDelimiter("[^\\d]+").hasNextLong()) {
                ts = scanner.nextLong();
            }
            scanner.close();
        }
        return ts;
    }

    /**
     * Computes the levels from qs in an {@link HashCode}
     */
    public static HashCode computeLevelHash(Map<String, String> qs) {
        Hasher hasher = hashFunction.newHasher();
        return qs == null ? hasher.putString("").hash() : hasher
                .putString(NullSafeUtils.getStringOrDefault(qs.get(Hit.JSON_QS_CMS_S1), ""))
                .putString(NullSafeUtils.getStringOrDefault(qs.get(Hit.JSON_QS_CMS_S2), ""))
                .putString(NullSafeUtils.getStringOrDefault(qs.get(Hit.JSON_QS_CMS_S3), ""))
                .putString(NullSafeUtils.getStringOrDefault(qs.get(Hit.JSON_QS_CMS_S4), ""))
                .putString(NullSafeUtils.getStringOrDefault(qs.get(Hit.JSON_QS_CMS_S5), ""))
                .putString(NullSafeUtils.getStringOrDefault(qs.get(Hit.JSON_QS_CMS_S6), ""))
                .putString(NullSafeUtils.getStringOrDefault(qs.get(Hit.JSON_QS_CMS_SN), ""))
                .putString(NullSafeUtils.getStringOrDefault(qs.get(Hit.JSON_QS_CMS_GR), ""))
                .putString(NullSafeUtils.getStringOrDefault(qs.get(Hit.JSON_QS_DOM), ""))
                .hash();
    }

    /**
     * Convert bit masks to a string representing ranges of percent of video seen by a user
     * <pre>
     *     "111" -> "1-3"
     *     "0011000011" -> "3-4,9-10"
     * </pre>
     */
    public static String getPercentFromMasks(String bitMask) {
        String result = "";
        int startIndex = -1;
        int lastIndex = -1;
        for (int i = 1; i <= bitMask.length(); i++) {
            if (bitMask.charAt(i - 1) == '1') {
                if (startIndex == -1) {
                    startIndex = i;
                }
                lastIndex = i;
                if (i == bitMask.length() && startIndex != -1) {
                    result += startIndex == lastIndex ? startIndex + "," : startIndex + "-" + lastIndex + ",";
                }
            } else {
                if (startIndex != -1) {
                    result += startIndex == lastIndex ? startIndex + "," : startIndex + "-" + lastIndex + ",";
                }
                startIndex = -1;
                lastIndex = -1;
            }
        }
        if (result.length() > 0) {
            return result.substring(0, result.length() - 1);
        } else {
            return "0";
        }
    }
}
