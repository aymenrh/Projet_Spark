package fr.mediametrie.internet.streaming.util;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * Provide commons hash related methods.
 */
public class HashUtils {

    /**
     * Compute a hash of the given fields using the murmur 3 128 algorithm and returning a long.
     * WARNING: As the murmur3 128 returns a Hash using 128 bits, this methods only returns the first 8 bytes as a long.
     */
    public static long Murmur3AsLong(String... fields) {
        Hasher hasher = Hashing.murmur3_128().newHasher();
        for (String field : fields) {
            if (field != null) {
                hasher.putString(field);
            }
        }
        return hasher.hash().asLong();
    }
}
