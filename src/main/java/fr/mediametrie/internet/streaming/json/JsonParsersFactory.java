package fr.mediametrie.internet.streaming.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Provides method to get json parsed dependening of the need.
 */
public class JsonParsersFactory {

    protected static ObjectMapper objectMapper = new ObjectMapper(new CustomParserJsonFactory(DowncasingParser::new))
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    protected static ObjectMapper multilevelsMapper = new ObjectMapper()
            .setPropertyNamingStrategy(new UppercasePropertyNamingStrategy())
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    protected static ObjectMapper sessionsMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Returns a jackson {@link ObjectMapper} configured to not fail on unknown properties, to allow
     * backslash escaping and to lowercase all the properties when parsing json.
     */
    public static ObjectMapper getJacksonJsonParser() {
        return objectMapper;
    }

    /**
     * Returns a jackson {@link ObjectMapper} configured write.
     * The returned mapper does not include empty fields in the serialized json.
     */
    public static ObjectMapper getMultilevelMapper() {
        return multilevelsMapper;
    }

    /**
     * Returns a jackson {@link ObjectMapper} configured to read write sessions as json.
     */
    public static ObjectMapper getSessionsMapper() {
        return sessionsMapper;
    }
}
