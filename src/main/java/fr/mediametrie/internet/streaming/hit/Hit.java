package fr.mediametrie.internet.streaming.hit;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Commons behavious
 */
public interface Hit extends Serializable {

    String HOST_TO_COMPACT_QUERY = "c.estat.com";

    // Provide all the json properties names.
    String JSON_UNIQUE_ID = "unique_id";
    String JSON_SERIAL = "serial";
    String JSON_TIMESTAMP = "timestamp";
    String JSON_REFERER = "referer";
    String JSON_UA = "user_agent";
    String JSON_IP = "remote_ip";
    String JSON_HOST = "host";
    String JSON_URI = "uri";
    String JSON_METHOD = "method";
    String JSON_STATUS = "status";
    String JSON_PROTOCOL = "protocol";
    String JSON_CONTENT_LENGTH = "contentLength";
    String JSON_HEADERS = "headers";
    String JSON_COOKIES_E = "e";
    String JSON_QS = "qs";
    String JSON_QS_TS = "ts";
    String JSON_QS_CMSVI = "cmsvi";
    String JSON_QS_CMSRK = "cmsrk";
    String JSON_QS_UID = "uid";
    String JSON_QS_CMS_S1 = "cmss1";
    String JSON_QS_CMS_S2 = "cmss2";
    String JSON_QS_CMS_S3 = "cmss3";
    String JSON_QS_CMS_S4 = "cmss4";
    String JSON_QS_CMS_S5 = "cmss5";
    String JSON_QS_CMS_S6 = "cmss6";
    String JSON_QS_CMS_SN = "cmssn";
    String JSON_QS_CMS_GR = "cmsgr";
    String JSON_QS_CMS_DU = "cmsdu";
    String JSON_QS_CMS_RK = "cmsrk";
    String JSON_QS_CMS_OP = "cmsop";
    String JSON_QS_CMS_PO = "cmspo";
   //****
    String JSON_QS_CMS_EV = "cmsev";
    String JSON_QS_CMS_PS = "cmsps";
    String JSON_QS_DOM = "dom";
    String JSON_QS_ML1 = "ml1";
    String JSON_QS_ML2 = "ml2";
    String JSON_QS_ML3 = "ml3";
    String JSON_QS_ML4 = "ml4";
    String JSON_QS_ML5 = "ml5";
    String JSON_QS_ML6 = "ml6";
    String JSON_QS_ML7 = "ml7";
    String JSON_QS_ML8 = "ml8";
    String JSON_QS_ML9 = "ml9";
    String JSON_QS_ML10 = "ml10";
    String JSON_QS_ML11 = "ml11";
    String JSON_QS_MSCID = "mscid";
    String JSON_QS_MSDM = "msdm";
    String JSON_QS_MSCH = "msch";
    String JSON_QS_MICH = "mich";
    String JSON_QS_TT = "tt";
    String JSON_COOKIE = "e";
    String JSON_GEOIP = "geoip";
    String JSON_GEOIP_COUNTRY_CODE = "country_code";
    String JSON_COOKIES = "cookies";
    String JSON_ESTAT = "estat";
    String JSON_CMS_PLAY = "cms_play";
    String JSON_METRICS = "metrics";
    String JSON_STREAMING = "streaming";

    /**
     * @return the querystring timestamp in seconds for this hit or null if no qs.ts was present in the json.
     */
    Long getTs();

    /**
     * @return the timestamp in millis for this hit or null if no timestamp was present in the json.
     */
    Long getTimestamp();

    default String getHost() {
        return "";
    }

    /**
     * Returns the first valid timestamp (in seconds) taken from
     * <ul>
     * <li>querystring ts (qs.ts) if defined</li>
     * <li>timestamp if defined</li>
     * <li>an empty optional</li>
     * </ul>
     */
    @JsonIgnore
    default Optional<Instant> getHitTimestamp() {
        if (getTs() != null) {
            return Optional.of(Instant.ofEpochSecond(getTs()));
        } else if (getTimestamp() != null) {
            return Optional.of(Instant.ofEpochMilli(getTimestamp()));
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return the first valid timestamp in seconds using the same algorithm than {@link #getHitTimestamp()}
     */
    @JsonIgnore
    default Optional<Long> getHitTimestampInSeconds() {
        Long ts = getTs() != null ? getTs() : getTimestamp() == null ? null : getTimestamp() / 1000;
        return Optional.ofNullable(ts);
    }

    /**
     * @return the Long parsed value from the given input or null if null, empty or not a number.
     */
    default Long parseLong(String value) {
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * @return the Integer parsed value from the given input or null if null, empty or not a number.
     */
    default Integer parseInteger(String value) {
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @JsonIgnore
    default Boolean isCompactQuery() {
        return HOST_TO_COMPACT_QUERY.equals(getHost());
    }
}
