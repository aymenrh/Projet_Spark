package fr.mediametrie.internet.streaming.util;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to fetch application properties such as version, name ... (essentially Maven properties)
 */
public class ApplicationProperties {

    /**
     * Predefined property that can be read from the application properties.
     */
    public enum Property {

        /**
         * The application group (corresponds to the maven groupId
         */
        GROUP("app.group"),

        /**
         * The application name (corresponds to the maven artifactId)
         */
        NAME("app.name"),

        /**
         * The application version (corresponds to the maven artifact version)
         */
        VERSION("app.version");

        String propertyName;

        private Property(String propertyName) {
            this.propertyName = propertyName;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationProperties.class);

    private static final String RESOURCE_NAME = "META-INF/streaming-app-version.properties";

    protected static final Supplier<URL> DEFAULT_RESOURCE_SUPPLIER = () -> ApplicationProperties.class.getClassLoader()
            .getResource(RESOURCE_NAME);

    static Supplier<URL> RESOURCE_SUPPLIER = DEFAULT_RESOURCE_SUPPLIER;

    /**
     * Return the value of the given property in the application properties from the current thread's resources
     * 
     * @param property The {@link Property} to fetch the value of
     * @return The value of the given property or null if it could not be found
     */
    public static String get(Property property) {
        return get(property.propertyName);
    }

    /**
     * Return the value of the given property in the application properties from the current thread's resources
     * 
     * @param property The property to fetch the value of
     * @return The value of the given property or null if it could not be found
     */
    public static String get(String property) {
        URL resource = RESOURCE_SUPPLIER.get();
        if (resource == null) {
            return null;
        }

        try (InputStream is = resource.openStream()) {
            if (is != null) {
                Properties properties = new Properties();
                properties.load(is);
                return properties.getProperty(property);
            }
        } catch (Exception e) {
            LOGGER.warn("Unable to load application properties. Cause : " + e.getMessage());
        }

        return null;
    }

}
