package fr.mediametrie.internet.streaming.preprocess;

/**
 * Provides a way to return a requested object/instance with additional information about instance creation.
 * This is used when a Factory creates objects or return already created objects (cache, ...) to inform the the
 * returned has been created during the call or in a previuos factory call.
 * <p>We introduced this feature to use spark accumulators to count how many time a factory create a new object.</p>
 */
public class FactoryResponse<T> {

    T obj;

    boolean created = false;

    public FactoryResponse(T obj, boolean created) {
        this.obj = obj;
        this.created = created;
    }

    /**
     * Creates a {@link FactoryResponse} with the given object as values and with created flag set to true.
     */
    public static <T> FactoryResponse<T> created(T obj) {
        return new FactoryResponse<T>(obj, true);
    }

    /**
     * Creates a {@link FactoryResponse} with the given object as values and with created flag set to false.
     */
    public static <T> FactoryResponse<T> cached(T obj) {
        return new FactoryResponse<T>(obj, false);
    }

    public T getValue() {
        return obj;
    }

    /**
     * @return if the associated value has been created during the last call or was returned from a cached value.
     */
    public boolean wasCreated() {
        return created;
    }

}
