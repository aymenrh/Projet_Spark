package fr.mediametrie.internet.streaming.hit;

import org.apache.spark.api.java.function.Function2;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Generate a 2 characters (hexa) hash based on a serial and a date.
 * The hash is used to prefix S3 keys to optimize storage performance
 */
public class ShortTermStorageHashProvider implements Function2<String, String, String> {

    private static final HashFunction HASHER = Hashing.murmur3_32();

    /**
     * Generate a 2 characters hash based on a serial and a date
     *
     * @param date
     * @param serial
     * @return
     * @throws Exception
     */
    @Override
    public String call(String date, String serial) throws Exception {
        return HASHER.hashString(serial + date).toString().substring(0, 2).toUpperCase();
    }
}
