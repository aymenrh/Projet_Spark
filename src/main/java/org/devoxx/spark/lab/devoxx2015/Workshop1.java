package org.devoxx.spark.lab.devoxx2015;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;

/**
 * Calcule la moyenne, le min, le max et le nombre de votes de l'utilisateur n°200.
 */
public class Workshop1 {

    public void run() throws URISyntaxException {
        SparkConf conf = new SparkConf().setAppName("Workshop").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
       // String ratingsPath = Paths.get(getClass().getResource("ratings.txt").getPath()).toString();

        Workshop1 runner = new Workshop1();
        Path path = runner.getFilePath(runner.getFile());

        JavaRDD<Rating> ratings = sc.textFile(path.toString())
                .map(line -> line.split("\\t"))
                .map(row -> new Rating(
                        Long.parseLong(row[0]),
                        Long.parseLong(row[1]),
                        Integer.parseInt(row[2]),
                        LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(row[3]) * 1000), ZoneId.systemDefault())

                ));


        JavaRDD<Rating> cachedRatingsForUser = ratings
                .filter(rating -> rating.user == 200)
                .cache();

        double max = cachedRatingsForUser
                .mapToDouble(rating -> rating.user)
                .max(Comparator.<Double>naturalOrder());

        double count = cachedRatingsForUser
                .count();

        cachedRatingsForUser.unpersist(false);

        System.out.println("count: " + count);
        System.out.println("max: " + max);

    }
    private File getFile() {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource("ratingsBig.txt").getFile());
    }

    private Path getFilePath(File file) {
        Path path = Paths.get(file.toURI());
        return path;
    }

    public static void main(String... args) throws URISyntaxException {
        new Workshop1().run();
    }
}
