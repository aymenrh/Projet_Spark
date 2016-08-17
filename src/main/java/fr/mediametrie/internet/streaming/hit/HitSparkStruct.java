package fr.mediametrie.internet.streaming.hit;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Holds the schema of a hit, used for Spark Dataframes
 */
public class HitSparkStruct {

    /**
     * Create a StructType representing the schema of a hit
     * 
     * @return
     */
    public static StructType getHitSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("hash", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("serial", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("unique_id", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("uri", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("remote_ip", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("user_agent", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("timestamp", DataTypes.LongType, false));
        structFields.add(DataTypes.createStructField("referer", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("host", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("method", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("status", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("protocol", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("content_length", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("headers",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
        structFields.add(DataTypes.createStructField("qs",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false));
        structFields.add(DataTypes.createStructField("geoip",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false));
        structFields.add(DataTypes.createStructField("cookies",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
        structFields.add(DataTypes.createStructField("estat",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false));
        structFields.add(DataTypes.createStructField("metrics",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false));
        return DataTypes.createStructType(structFields);
    }
}
