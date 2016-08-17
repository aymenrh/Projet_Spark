package org.mediametrie.tools;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.mediametrie.tools.constant.DataSourceType;

/**
 * Created by MRAIHIT on 21/06/2016.
 */
public class DataLoader {

    private SQLContext sqlContext;

    private DataLoader(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }


    /**
     *
     * @param file
     * @param type
     * @return
     */
    public static DataLoader createDataLoader(String file,DataSourceType type) {

        SparkConf conf = new SparkConf().setAppName("PoTools").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        String path = DataLoader.class.getResource(file).getPath();
        DataFrame hits = sqlContext.load(path, "json");
        //System.err.println(hits.first());
        sqlContext.registerDataFrameAsTable(hits, "streaming_hit");
        return new DataLoader(sqlContext);
    }


    public SQLContext getSqlContext() {
        return sqlContext;
    }
}
