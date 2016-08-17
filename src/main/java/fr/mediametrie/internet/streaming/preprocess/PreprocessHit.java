package fr.mediametrie.internet.streaming.preprocess;

import java.io.Serializable;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import fr.mediametrie.internet.streaming.hit.Hit;
import fr.mediametrie.internet.streaming.hit.HitUtils;

/**
 * Contains the raw representation of an incoming hit to preprocess (json) and propoerties
 * needed for preprocess execution flow.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PreprocessHit implements Hit, Serializable {

    @JsonProperty(JSON_IP)
    public String ip;

    @JsonProperty(JSON_TIMESTAMP)
    public Long timestamp;

    public Long ts;

    public String cmsVi;

    public Integer rank;

    public String raw;

    public Integer cmsPO;

    public Integer cmsDU;
    
    //*****
    public Integer cmsOP;
    public Integer cmsEV;


    public boolean valid = true;

    public PreprocessMetrics metrics = new PreprocessMetrics();

    public PreprocessHit() {
    }

    public PreprocessHit(String raw, boolean valid) {
        this.raw = raw;
        this.valid = valid;
    }

    public PreprocessHit(String raw, String cmsVi, String ip) {
        this.cmsVi = cmsVi;
        this.ip = ip;
        this.raw = raw;
    }

    public String getIp() {
        return ip;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public Long getTs() {
        return ts;
    }

    public Integer getCmsPO() {
        return cmsPO;
    }
    public void setCmsPO(Integer cmsPO) {
        this.cmsPO = cmsPO;
    }
    
    //******
    public Integer getCmsOP() {
        return cmsOP;
    }
    public void setCmsOP(Integer cmsOP) {
        this.cmsPO = cmsOP;
    }
    
    public Integer getCmsEV() {
        return cmsEV;
    }
    public void setCmsEV(Integer cmsEV) {
        this.cmsEV = cmsEV;
    }
    //*** end commentaire
    
    public Integer getCmsDU() {
        return cmsDU;
    }

    public void setCmsDU(Integer cmsDU) {
        this.cmsDU = cmsDU;
    }
    /**
     * @return the cmsvi for this hit or empty string if not defined.
     */
    public String getCmsViOrEmpty() {
        return cmsVi == null ? "" : cmsVi;
    }

    /**
     * @return the rank for this hit or 0 if not defined.
     */
    private int getRankOrZero() {
        return rank == null ? 0 : rank;
    }

    /**
     * Used for json deserialization to retrieve only needed properties from the qs sub object.
     */
    public void setQs(Map<String, Object> qs) {
        cmsDU = Integer.valueOf((String) qs.get(JSON_QS_CMS_DU));
        cmsPO = Integer.valueOf((String) qs.get(JSON_QS_CMS_PO));
        //***
        cmsOP = Integer.valueOf((String) qs.get(JSON_QS_CMS_OP));
        if(((String) qs.get(JSON_QS_CMS_EV)).equals("polling"))
        {
        	cmsEV = 5;
        }
        if(((String) qs.get(JSON_QS_CMS_EV)).equals("pause"))
        {
        	cmsEV = 7;
        }
        if(((String) qs.get(JSON_QS_CMS_EV)).equals("stop"))
        {
        	cmsEV = 12;
        }
  
        //***
        cmsVi = (String) qs.get(JSON_QS_CMSVI);
        rank = parseInteger((String) qs.get(JSON_QS_CMSRK));
        ts = HitUtils.extractTimestamp((String) qs.get(JSON_QS_TS));
    }

    public String getRaw() {
        return raw;
    }

    public void setRaw(String s) {
        this.raw = s;
    }

    public boolean isValid() {
        return valid;
    }

    public boolean isInvalid() {
        return !valid;
    }

    @Override
    public String getHost() {
        return "";
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("ip", ip)
                .add("timestamp", timestamp)
                .add("ts", ts)
                .add("cmsVi", cmsVi)
                .add("rank", rank)
                .add("raw", raw)
                .toString();
    }

    public static Comparator<PreprocessHit> getComparator() {
        return (h1, h2) -> {
            int compareResult;
            // check cmsvi first, then rank, then timestamp
            if ((compareResult = h1.getCmsViOrEmpty().compareTo(h2.getCmsViOrEmpty())) != 0) {
                return compareResult;
            } else if ((compareResult = Integer.compare(h1.getRankOrZero(), h2.getRankOrZero())) != 0) {
                return compareResult;
            } else {
                return h1.getHitTimestamp().orElse(Instant.MIN).compareTo(h2.getHitTimestamp().orElse(Instant.MIN));
            }
        };
    }

}
