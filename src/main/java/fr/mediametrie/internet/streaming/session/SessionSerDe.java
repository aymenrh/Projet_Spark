package fr.mediametrie.internet.streaming.session;

import static fr.mediametrie.internet.streaming.session.SessionSerDe.ValueType.INT;
import static fr.mediametrie.internet.streaming.session.SessionSerDe.ValueType.LONG;
import static fr.mediametrie.internet.streaming.session.SessionSerDe.ValueType.STRING;

import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows to serialize and de-serialize a {@link Session}. Format is proprietary and DOES NOT yet handles changes in
 * versions.
 *
 */
public class SessionSerDe {

    private static Logger LOGGER = LoggerFactory.getLogger(SessionSerDe.class);

    static final String SEPARATOR = "\001";
    static final String NULL_VALUE = "\002";

    enum ValueType {
        LONG, INT, BOOLEAN, STRING;
    }

    public static String serialize(Session s) {
        StringBuilder sb = new StringBuilder();
        appendString(sb, s.hash);
        appendString(sb, s.cmsS1);
        appendString(sb, s.cmsS2);
        appendString(sb, s.cmsS3);
        appendString(sb, s.cmsS4);
        appendString(sb, s.cmsS5);
        appendString(sb, s.cmsGR);
        appendString(sb, s.cmsSN);
        appendString(sb, s.dom);
        appendString(sb, s.cmsVi);
        appendString(sb, s.serial);
        appendString(sb, s.cookie);
        appendString(sb, s.ip);
        appendString(sb, s.ua);
        sb.append(s.beginTimestamp).append(SEPARATOR);
        sb.append(s.endTimestamp).append(SEPARATOR);
        append(sb, s.ts);
        append(sb, s.timestamp);
        append(sb, s.rank);
        appendString(sb, s.host);
        sb.append(s.consumptionDuration).append(SEPARATOR);
        sb.append(s.streamDuration).append(SEPARATOR);
        sb.append(s.visitorId).append(SEPARATOR);
        int hitCount = s.hits == null ? 0 : s.hits.size();
        sb.append(hitCount).append(SEPARATOR);
        for (int i = 0; i < hitCount; ++i) {
            sb.append(serializeHit(s.hits.get(i))).append(SEPARATOR);
        }

        return sb.toString();
    }

    public static Session deserialize(String serialized) {
        Session s = new Session();
        try (Scanner scanner = new Scanner(serialized)) {
            scanner.useDelimiter(SEPARATOR);

            s.hash = scanNullable(scanner, STRING);
            s.cmsS1 = scanNullable(scanner, STRING);
            s.cmsS2 = scanNullable(scanner, STRING);
            s.cmsS3 = scanNullable(scanner, STRING);
            s.cmsS4 = scanNullable(scanner, STRING);
            s.cmsS5 = scanNullable(scanner, STRING);
            s.cmsGR = scanNullable(scanner, STRING);
            s.cmsSN = scanNullable(scanner, STRING);
            s.dom = scanNullable(scanner, STRING);
            s.cmsVi = scanNullable(scanner, STRING);
            s.serial = scanNullable(scanner, STRING);
            s.cookie = scanNullable(scanner, STRING);
            s.ip = scanNullable(scanner, STRING);
            s.ua = scanNullable(scanner, STRING);

            s.beginTimestamp = scanner.nextLong();
            s.endTimestamp = scanner.nextLong();

            s.ts = scanNullable(scanner, LONG);
            s.timestamp = scanNullable(scanner, LONG);
            s.rank = scanNullable(scanner, INT);
            s.host = scanNullable(scanner, STRING);

            s.consumptionDuration = scanner.nextInt();
            s.streamDuration = scanner.nextInt();
            s.visitorId = scanner.nextLong();

            int hitCount = scanner.nextInt();
            for (int i = 0; i < hitCount; ++i) {
                s.addHit(deserializeHit(scanner));
            }
        } catch (Exception e) {
            LOGGER.warn("Error during session deserialize : {} using value '{}'", e.getMessage(), serialized);
            return null;
        }
        return s;
    }

    private static StringBuilder serializeHit(SessionHit h) {
        StringBuilder sb = new StringBuilder();
        sb.append(h.tsInSeconds).append(SEPARATOR);
        sb.append(h.cmsRk).append(SEPARATOR);
        sb.append(h.position).append(SEPARATOR);
        sb.append(h.oldPosition).append(SEPARATOR);
        sb.append(h.duration).append(SEPARATOR);
        sb.append(h.playState).append(SEPARATOR);
        sb.append(h.isCompactQuery);
        return sb;
    }

    private static SessionHit deserializeHit(Scanner scanner) {
        SessionHit h = new SessionHit();
        h.tsInSeconds = scanner.nextLong();
        h.cmsRk = scanner.nextInt();
        h.position = scanner.nextInt();
        h.oldPosition = scanner.nextInt();
        h.duration = scanner.nextInt();
        h.playState = scanner.nextInt();
        h.isCompactQuery = scanner.nextBoolean();
        return h;
    }

    private static <T> StringBuilder append(StringBuilder destination, T object) {
        if (object == null) {
            return destination.append(NULL_VALUE).append(SEPARATOR);
        }
        return destination.append(object).append(SEPARATOR);
    }

    private static StringBuilder appendString(StringBuilder destination, String str) {
        if (str == null) {
            return destination.append(NULL_VALUE).append(SEPARATOR);
        }
        return destination.append(str.replaceAll("\n", "\\n")).append(SEPARATOR);
    }

    @SuppressWarnings("unchecked")
    private static <T> T scanNullable(Scanner scanner, ValueType type) {
        String value = scanner.next();
        if (NULL_VALUE.equals(value)) {
            return null;
        }
        switch (type) {
        case INT:
            return (T) Integer.valueOf(value);
        case LONG:
            return (T) Long.valueOf(value);
        case BOOLEAN:
            return (T) Boolean.valueOf(value);
        default:
            return (T) value.replaceAll("\\n", "\n");
        }
    }

}
