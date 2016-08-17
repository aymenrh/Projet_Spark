package fr.mediametrie.internet.streaming.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Configure DateTime time zone for all programs.
 * <p>
 *     This class provide various {@link DateTimeFormatter} configured with the CET zone id.
 *     Those formatters patterns should not be updated as they defines commons international formatd.
 *     If a new pattern or zone is needed for another usage, define a new one for this specific usage.
 * </p>
 */
public class DateTimeUtils {

    // Central European Time
    public static final String TIME_ZONE = "CET";

    public static final ZoneId ZONE_ID = ZoneId.of(TIME_ZONE);

    /**
     * Formatter with format <code>yyyy-MM-dd</code>
     */
    public static final DateTimeFormatter INTERNATIONAL_DATE = generateFormatter("yyyy-MM-dd");

    /**
     * Formatter with format <code>yyyyMMdd</code>
     */
    public static final DateTimeFormatter INTERNATIONAL_DATE_NO_DASH = generateFormatter("yyyyMMdd");

    /**
     * Formatter with format <code>yyyy-MM-dd HH:mm:ss</code>
     */
    public static final DateTimeFormatter INTERNATIONAL_TIME = generateFormatter("yyyy-MM-dd HH:mm:ss");

    /**
     * Formatter with format <code>yyyy-MM-dd HH:mm:ss.SSS</code>
     */
    public static final DateTimeFormatter INTERNATIONAL_TIME_MILLI = generateFormatter("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Formatter with format <code>yyyyMMdd-HHmmss</code>
     * This constant should not be changed as it defines commons international format. If a new pattern or
     * zone is needed for another usage, define a new {@link DateTimeFormatter} for this specific usage.
     */
    public static final DateTimeFormatter INTERNATIONAL_DATE_TIME_COMPACT = generateFormatter("yyyyMMdd-HHmmss");

    /**
     * Generates new DateTimeFormatter with common configuration for the given format.
     * 
     * @param format DateTime format
     * @return DateTimeFormatter
     */
    private static DateTimeFormatter generateFormatter(String format) {
        return DateTimeFormatter.ofPattern(format).withZone(ZONE_ID);
    }

    /**
     * Returns the epoch second timestemps correspondng to the given date at midnight.
     * This uses the zone defined in {@link #TIME_ZONE}.
     */
    public static long epochSecondMidnight(LocalDate day) {
        return day.atStartOfDay(ZONE_ID).toEpochSecond();
    }

    /**
     * Returns the epoch second timestamp correspondng to midnight for the day corresponding to the given epoch
     * timestamp in millis.
     * This uses the zone defined in {@link #TIME_ZONE}.
     */
    public static long epochSecondMidnight(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atZone(ZONE_ID).toLocalDate().atStartOfDay(ZONE_ID)
                .toEpochSecond();
    }

    /**
     * Return {@link java.time.LocalDateTime} corresponding to the given epoch timestamp in second.
     */
    public static LocalDateTime dateTimeFromEpochSecond(long epochSecond){
        return Instant.ofEpochSecond(epochSecond).atZone(ZONE_ID).toLocalDateTime();
    }
}
