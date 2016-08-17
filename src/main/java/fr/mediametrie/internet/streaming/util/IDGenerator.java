package fr.mediametrie.internet.streaming.util;

import java.util.UUID;

/**
 * Simple implementation to generate an UID using UUID jdk classe.
 *
 * Warning: UID differs from UUID see https://en.wikipedia.org/wiki/Universally_unique_identifier
 *
 * As an improvement for a better ids generator to avoid collisions the following github project should
 * be investigated:
 * https://github.com/mumrah/flake-java
 * https://github.com/relops/snowflake
 */
public class IDGenerator {

    private static final IDGenerator INSTANCE = new IDGenerator();

    public static IDGenerator get() {
        return INSTANCE;
    }

    public long next() {
        return UUID.randomUUID().getMostSignificantBits();
    }
}
