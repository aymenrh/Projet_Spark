package fr.mediametrie.internet.streaming.util;

/**
 * Compute a shard of a given input. This shard is used to distribute hits on a set of
 * front dbs.
 */
public interface Sharder {

    /**
     * @return a shard with a value between 1 and shardCount for the given input.
     */
    int shard(String input, int shardCount);
}
