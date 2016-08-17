package org.devoxx.spark.lab.devoxx2015;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Requête SQL sur un DataFrame chargé depuis un fichier JSON.
 */
public class Workshop3 {

    public static void main(String... args) {
        SparkConf conf = new SparkConf().setAppName("Workshop").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        String path = Workshop3.class.getResource("/20160419-00-15013.json").getPath();
        DataFrame hits = sqlContext.load(path, "json");
        System.err.println(hits.first());

        sqlContext.registerDataFrameAsTable(hits, "streaming_hit");
        DataFrame frame = sqlContext.sql("SELECT * FROM streaming_hit where unique_id=\"VxVmCcCoAL0AAVeIgvoAABw@\"");
        System.out.println(frame.first());
    }
}
