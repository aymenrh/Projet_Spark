package fr.mediametrie.internet.streaming.util;

import java.io.Serializable;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Implementation of the {@link Sharder} using the murmur3 32 hash function.
 * <p>This implementation is simpler that the one using the murmur3 128 because it avoids using BigInteger
 * to manage the hash result.</p>
 */
public class Murmur32Sharder implements Sharder, Serializable {

    private static final String NULL_VALUE = "NULL";

    HashFunction hashFunction = Hashing.murmur3_32();

    @Override
    public int shard(String input, int shardCount) {
        if (input == null) {
            return shard(NULL_VALUE, shardCount);
        }
        HashCode hashCode = hashFunction.newHasher().putString(input).hash();
        int hashValue = Math.abs(hashCode.asInt());
        return (hashValue % shardCount) + 1;
    }
}
