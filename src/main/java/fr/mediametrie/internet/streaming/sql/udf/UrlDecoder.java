package fr.mediametrie.internet.streaming.sql.udf;

import org.apache.commons.codec.net.URLCodec;
import org.apache.spark.sql.api.java.UDF1;

/**
 * UDF for URL Decoding
 */
public class UrlDecoder implements UDF1<String,String> {

    private static URLCodec codec = new URLCodec();

    @Override
    public String call(String t1) {
        if(t1 == null) {
            return null;
        }

        try {
            return codec.decode(t1, "UTF-8");
        }catch(Exception e) {
            // If decode Fail, return the same Input
            return t1;
        }
    }
}
