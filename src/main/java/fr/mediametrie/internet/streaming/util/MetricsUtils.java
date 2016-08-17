package fr.mediametrie.internet.streaming.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Provide utilities methods to manage metrics.
 */
public class MetricsUtils {

    private static Logger LOGGER = LoggerFactory.getLogger(MetricsUtils.class);

    private static ObjectMapper metricsMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneId.of("CET"));

    /**
     * Publish the given metrics to the given output dir.
     * Filename pattern is '{outputDir}/{app}-{runId}.json'.
     */
    public static void publishBatchMetrics(Object metrics, String outputDir, String app, String runId)
            throws IOException {
        LOGGER.info("Publishing metrics to output dir {}", outputDir);
        Path filePath = new Path(outputDir, buildFileName(app, runId));
        FileSystem fileSystem = filePath.getFileSystem(new Configuration());
        try (FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
                PrintWriter writer = new PrintWriter(fsDataOutputStream)) {
            metricsMapper.writeValue(writer, metrics);
        }
    }

    /**
     * Publish the given metrics to the given output dir.
     * Filename pattern is '{outputDir}/{app}-{current time using yyyyMMdd_HHmmss pattern}.json'.
     */
    public static void publishStreamingMetrics(Object metrics, String outputDir, String app)
            throws IOException {
        LOGGER.info("Publishing metrics to output dir {}", outputDir);
        String currentTime = LocalDateTime.now().format(dtf);
        Path filePath = new Path(outputDir, buildFileName(app, currentTime));
        FileSystem fileSystem = filePath.getFileSystem(new Configuration());
        try (FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
                PrintWriter writer = new PrintWriter(fsDataOutputStream)) {
            metricsMapper.writeValue(writer, metrics);
        }
    }

    public static String buildFileName(String app, String runId) {
        return app + "-" + runId + ".json";
    }
}
