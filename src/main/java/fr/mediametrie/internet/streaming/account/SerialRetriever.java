package fr.mediametrie.internet.streaming.account;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Convenient class to handle relatives information to serial.
 * Must be used to retrieve Cumul Account and list of Serials.
 */
public class SerialRetriever {

    private static Logger LOGGER = LoggerFactory.getLogger(SerialRetriever.class);

    public SerialRetriever() {
    }

    /**
     * @param file the file containing the serials to process as a Json array
     * @return The set of serials to Process
     * @throws IOException
     */
    public Set<String> getSerialsSetFromFile(String file) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(file), new Configuration());
        InputStream in = fs.open(new Path(file));
        return getSerialsFromJsonArray(in);
    }

    /**
     *
     * @param file the file containing the cumul Accounts to process as a Json array
     * @return The map of cumul accounts
     * @throws IOException
     */
    public Map<String, Set<String>> getCumulsSetFromFile(String file) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(file), new Configuration());
        InputStream in = fs.open(new Path(file));
        return getCumulAccountsFromJsonArray(in);
    }


    /**
     * Build the list of cumul accounts based on a jsonArray
     *
     * @param jsonArray the json array containing the element : "serial associated with a list of parents (cumul)
     * @return A map of serial as key and the list of cumul accounts as value. If the file cannot be parse
     * for any reason, an empty Map is returned.
     */
    protected Map<String, Set<String>> getCumulAccountsFromJsonArray(InputStream jsonArray) throws IOException {
        Map<String, Set<String>> result = new HashMap<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JavaType type = mapper.getTypeFactory().constructCollectionType(List.class, CumulElement.class);
            List<CumulElement> elements = mapper.readValue(jsonArray, type);
            for (CumulElement element : elements) {
                // Remove serial from parents if any, and add only if the parents list is not empty.
                Set<String> set = new HashSet<>(element.getParents());
                set.remove(element.getSerial());
                if (!set.isEmpty()) {
                    result.put(element.getSerial(), set);
                }
            }
        } catch (JsonMappingException e) {
            LOGGER.warn("Invalid format for cumul accounts definition file (json).");
        }
        return result;
    }

    /**
     * Build the list of serials based on a jsonArray
     *
     * @param jsonArray the json array containing the element : serial.
     * @return The set of serial to process. If the InputStream cannot be parse, for any reasons,
     * an empty Set is returned.
     */
    protected Set<String> getSerialsFromJsonArray(InputStream jsonArray) throws IOException {
        Set<String> serialsSet = Collections.emptySet();
        if (jsonArray != null) {
            ObjectMapper mapper = new ObjectMapper();
            JsonFactory jsonFactory = new JsonFactory();
            JsonParser jsonParser = jsonFactory.createParser(jsonArray);
            TypeReference<Set<String>> tRef = new TypeReference<Set<String>>() {
            };
            try {
                serialsSet = mapper.readValue(jsonParser, tRef);
            } catch (JsonMappingException ex) {
                serialsSet.removeAll(Arrays.asList(null, ""));
            }
        }
        return serialsSet;
    }

    /**
     * Bean to deserialize json element for cumul account (in the form serial associated to a list of serials)
     */
    public static class CumulElement {
        @JsonProperty("serial")
        private String serial;

        @JsonProperty("parents")
        private List<String> parents;

        public void setSerial(String serial) {
            this.serial = serial;
        }

        public void setParents(List<String> parents) {
            this.parents = parents;
        }

        public String getSerial() {
            return serial;
        }

        public List<String> getParents() {
            return parents;
        }
    }

}
