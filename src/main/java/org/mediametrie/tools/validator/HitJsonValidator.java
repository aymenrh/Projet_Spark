package org.mediametrie.tools.validator;

import com.fasterxml.jackson.databind.ObjectMapper;

import fr.mediametrie.internet.streaming.json.JsonParsersFactory;
import fr.mediametrie.internet.streaming.preprocess.PreprocessHit;
import fr.mediametrie.internet.streaming.preprocess.PreprocessSparkFunctions;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.devoxx.spark.lab.devoxx2015.Rating;
import org.mediametrie.tools.DataLoader;
import org.mediametrie.tools.constant.DataSourceType;

import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by MRAIHIT on 21/06/2016.
 */
public class HitJsonValidator implements Serializable {

      private static String cmsVi = "1153933388130820096";

    public static void main(String... args) throws URISyntaxException {
        new HitJsonValidator().run();
    }
        public void run() {
            long startTime = System.currentTimeMillis();
            //DataLoader loader = DataLoader.createDataLoader("/20160419-00-15013.json", DataSourceType.JSON);

            showHitsFromCmsvi();
            long endTime = System.currentTimeMillis();
            System.out.println("Temps de traitement = " + (endTime - startTime)/1000 +" s");
        }


        private void showHitsFromCmsvi(){
            SparkConf conf = new SparkConf()
                    .setAppName("PoTools")
                    .setMaster("local[*]") //
                    .set("spark.driver.maxResultSize", "1500m")
                    .set("spark.executor.memory","1024m")
                    .set("spark.driver.memory","1024m");
            JavaSparkContext sparkContext = new JavaSparkContext(conf);


            Path path = getFilePath(getFile());


            JavaRDD<PreprocessHit> hits = sparkContext.textFile(path.toString()).map(
                    new Function<String, PreprocessHit>() {
                        public PreprocessHit call(String line) throws Exception {
                            try {
                                ObjectMapper mapper = JsonParsersFactory.getJacksonJsonParser();
                                PreprocessHit hit = mapper.readValue(line, PreprocessHit.class);
                                hit.raw = line;
                                return hit;
                            } catch (Exception e) {
                                PreprocessHit hit = new PreprocessHit(line, false);
                                return hit;
                            }
                        }
                    }
            );

            System.out.println("Il y a "+hits.collect().size()+" hits ");
            countCmsPOHigerThancmsDU(hits);
            //JE vais calculer la methode ici 
            calculDuree(hits);
        }

    /**
     * Comptabilise le nombre de hit dont la position est au dela de la dur√©e
     * @param hits
     */
    private void countCmsPOHigerThancmsDU(JavaRDD<PreprocessHit> hits) {
        int count = 0;
        for (PreprocessHit hit : hits.collect()) {
        	System.out.println("*******"+hit.getCmsEV());
            if (isHitValide(hit) && hit.getCmsPO() > hit.getCmsDU()){
                count++;
            }
        }
        System.out.println("Il y a "+count+" hits dont la position est superieure a† la duree");
        
    }

    public void calculDuree(JavaRDD<PreprocessHit> hits) {
    	Map<String, Integer> calc = new HashMap();
    	for(PreprocessHit hit : hits.collect())
    	{
    		if(hit.getCmsEV() == 5 || hit.getCmsEV()==12 ){
    		if(!calc.containsKey(hit.cmsVi)){
    			calc.put(hit.cmsVi, hit.getCmsPO()-hit.getCmsOP());		
    		}
    		else 
    		{
    			calc.put(hit.cmsVi,calc.get(hit.cmsVi)+(hit.getCmsPO()-hit.getCmsOP()));
    		}
    		}
    	}
    	for (Map.Entry<String, Integer> e: calc.entrySet()) {
			System.out.print("Code : "+e.getKey()+" value : "+e.getValue());
			
			System.out.println("\n");
		}
	}
    
    private boolean isHitValide(PreprocessHit hit) {
    return hit.getCmsPO() != null && hit.getCmsPO()>=0 && hit.getCmsDU() != null && hit.getCmsDU() >=0;
    }


    private File getFile() {
        ClassLoader classLoader = this.getClass().getClassLoader();
        // return new File(classLoader.getResource("hit.json").getFile());
        return new File(classLoader.getResource("20160419-00-15013.json").getFile());
    }

    private Path getFilePath(File file) {
        Path path = Paths.get(file.toURI());
        return path;
    }
}
