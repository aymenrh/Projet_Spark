package fr.mediametrie.internet.streaming.util;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Serializable version of {@link Function}.
 * @param <T> the input type
 * @param <R> the result type.
 */
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
