package fr.mediametrie.internet.streaming.json;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides utilities functions related to json.
 *
 */
public class JsonUtils {

    private static Logger LOGGER = LoggerFactory.getLogger(JsonUtils.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Compare 2 json trees in their String representation and return true if they are equals or false if not equals
     * or if a parsing exception occurs.
     */
    public static boolean compareJson(String json1, String json2) {
        try {
            return mapper.readTree(json1).equals(mapper.readTree(json2));
        } catch (IOException e) {
            LOGGER.warn("JSON parsing error {}", e.getMessage(), e);
            return false;
        }
    }
}
