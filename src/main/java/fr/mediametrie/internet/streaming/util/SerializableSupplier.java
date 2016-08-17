package fr.mediametrie.internet.streaming.util;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Serializable version of {@link java.util.function.Supplier}.
 * 
 * @param <T> the input type
 */
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
