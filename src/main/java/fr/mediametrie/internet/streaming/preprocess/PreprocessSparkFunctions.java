package fr.mediametrie.internet.streaming.preprocess;

import java.time.Duration;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.swrve.ratelimitedlogger.RateLimitedLogger;

import fr.mediametrie.internet.streaming.hit.JsonHit;
import fr.mediametrie.internet.streaming.json.JsonParsersFactory;

/**
 * Provide {@link Function} related to streaming preprocess.
 */
public class PreprocessSparkFunctions {

    private static Logger LOGGER = RateLimitedLogger.create(PreprocessSparkFunctions.class).maxRate(10)
            .every(Duration.ofMinutes(1)).build();

    /**
     * Returns a {@link Function} which builds a {@link PreprocessHit} based on a json representation of a hit.
     */
    public static Function<String, PreprocessHit> preprocessHitBuilder() {
        return s -> {
            try {
                ObjectMapper mapper = JsonParsersFactory.getJacksonJsonParser();
                PreprocessHit hit = mapper.readValue(s, PreprocessHit.class);
                hit.raw = s;
                hit.metrics.incomingHitCount = 1;
                hit.metrics.validHitCount = 1;
                return hit;
            } catch (Exception e) {
                LOGGER.warn("Error when parsing json during preprocess '{}'. Error is {} ", s, e.getMessage());
                PreprocessHit hit = new PreprocessHit(s, false);
                hit.metrics.incomingHitCount = 1;
                hit.metrics.invalidHitWithBadJsonCount = 1;
                return hit;
            }
        };
    }

    /**
     * Return a {@link Function} which enrichs a {@link PreprocessHit} providing a list of enrichment functions.
     * Precondition: The incoming hits must be not null (filter before if needed).
     */
    public static Function<PreprocessHit, PreprocessHit> enrichJson(
            List<Function2<PreprocessHit, ObjectNode, ObjectNode>> functionList) {
        return (hit -> {
            if (hit.valid) {
                ObjectMapper objMapper = JsonParsersFactory.getJacksonJsonParser();
                ObjectNode jsonObject = (ObjectNode) objMapper.readTree(hit.raw);
                for (Function2<PreprocessHit, ObjectNode, ObjectNode> f : functionList) {
                    jsonObject = f.call(hit, jsonObject);
                }
                hit.raw = objMapper.writeValueAsString(jsonObject);
            }
            return hit;
        });
    }

    /**
     * Return a {@link Function} in charge of converting PreprocessHit to dataframe rows.
     */
    public static Function<PreprocessHit, Row> mapHitToRow(
            Function2<String, String, String> longTermStorageHashProvider) {
        return hit -> {
            try {
                ObjectMapper mapper = JsonParsersFactory.getJacksonJsonParser();
                JsonHit genericHit = mapper.readValue(hit.raw, JsonHit.class);
                return null;
            } catch (Exception e) {
                LOGGER.warn("Error while generating spark dataframe row '{}'. Error is {} ", hit.raw, e.getMessage());
                return null;
            }
        };
    }
}
