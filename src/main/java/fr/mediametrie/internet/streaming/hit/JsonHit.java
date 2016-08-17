package fr.mediametrie.internet.streaming.hit;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import fr.mediametrie.internet.streaming.json.JsonParsersFactory;

/**
 * Bean used for serialization and deserialization of hit in raw json format.
 *
 * Jackson {@link com.fasterxml.jackson.databind.ObjectMapper} should be used to read/write and is available thru
 * the {@link fr.mediametrie.internet.streaming.json.JsonParsersFactory}.
 *
 * WARNING: do not use this bean to deserialize json in spark RDDs as its memory size is too big if the processing
 * only need a few fields from the json.
 */
public class JsonHit implements Hit {

    @JsonProperty("unique_id")
    public String uniqueId;

    @JsonProperty("uri")
    public String uri;

    @JsonProperty("remote_ip")
    public String remoteIp;

    @JsonProperty("user_agent")
    public String userAgent;

    @JsonProperty("timestamp")
    public Long timestamp;

    @JsonProperty("referer")
    public String referer;

    @JsonProperty("host")
    public String host;

    @JsonProperty("method")
    public String method;

    @JsonProperty("status")
    public String status;

    @JsonProperty("protocol")
    public String protocol;

    @JsonProperty("content_length")
    public String contentLength;

    @JsonProperty("headers")
    public Map<String, String> headers = new HashMap<>();

    @JsonProperty("qs")
    public Map<String, String> qs = new HashMap<>();

    @JsonProperty("geoip")
    public Map<String, String> geoip = new HashMap<>();

    @JsonProperty("cookies")
    public Map<String, String> cookies = new HashMap<>();

    @JsonProperty("estat")
    public Map<String, String> estat = new HashMap<>();

    @JsonProperty("metrics")
    public Map<String, String> metrics = new HashMap<>();

    /**
     * Parse the given json string to return a valid {@link JsonHit} or throw a {@link RuntimeException} if
     * the provided json string is invalid.
     */
    public static JsonHit fromJson(String json) {
        try {
            return JsonParsersFactory.getJacksonJsonParser().readValue(json, JsonHit.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Getter for field uniqueId
     *
     * @return uniqueId
     */
    public String getUniqueId() {
        return uniqueId;
    }

    /**
     * Setter for field uniqueId
     *
     * @param uniqueId new value for field uniqueId
     */
    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    /**
     * Getter for field uri
     *
     * @return uri
     */
    public String getUri() {
        return uri;
    }

    /**
     * Setter for field uri
     *
     * @param uri new value for field uri
     */
    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * Getter for field remoteIp
     *
     * @return remoteIp
     */
    public String getRemoteIp() {
        return remoteIp;
    }

    /**
     * Setter for field remoteIp
     *
     * @param remoteIp new value for field remoteIp
     */
    public void setRemoteIp(String remoteIp) {
        this.remoteIp = remoteIp;
    }

    /**
     * Getter for field userAgent
     *
     * @return userAgent
     */
    public String getUserAgent() {
        return userAgent;
    }

    /**
     * Setter for field userAgent
     *
     * @param userAgent new value for field userAgent
     */
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    /**
     * Getter for field timestamp
     *
     * @return timestamp
     */
    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * Setter for field timestamp
     *
     * @param timestamp new value for field timestamp
     */
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Getter for field referer
     *
     * @return referer
     */
    public String getReferer() {
        return referer;
    }

    /**
     * Setter for field referer
     *
     * @param referer new value for field referer
     */
    public void setReferer(String referer) {
        this.referer = referer;
    }

    /**
     * Getter for field host
     *
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * Setter for field host
     *
     * @param host new value for field host
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Getter for field method
     *
     * @return method
     */
    public String getMethod() {
        return method;
    }

    /**
     * Setter for field method
     *
     * @param method new value for field method
     */
    public void setMethod(String method) {
        this.method = method;
    }

    /**
     * Getter for field status
     *
     * @return status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Setter for field status
     *
     * @param status new value for field status
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Getter for field protocol
     *
     * @return protocol
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Setter for field protocol
     *
     * @param protocol new value for field protocol
     */
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    /**
     * Getter for field contentLength
     *
     * @return contentLength
     */
    public String getContentLength() {
        return contentLength;
    }

    /**
     * Setter for field contentLength
     *
     * @param contentLength new value for field contentLength
     */
    public void setContentLength(String contentLength) {
        this.contentLength = contentLength;
    }

    /**
     * Getter for field headers
     *
     * @return headers
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Setter for field headers
     *
     * @param headers new value for field headers
     */
    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    /**
     * Getter for field qs
     *
     * @return qs
     */
    public Map<String, String> getQs() {
        return qs;
    }

    /**
     * Setter for field qs
     *
     * @param qs new value for field qs
     */
    public void setQs(Map<String, String> qs) {
        this.qs = qs;
    }

    /**
     * Getter for field geoip
     *
     * @return geoip
     */
    public Map<String, String> getGeoip() {
        return geoip;
    }

    /**
     * Setter for field geoip
     *
     * @param geoip new value for field geoip
     */
    public void setGeoip(Map<String, String> geoip) {
        this.geoip = geoip;
    }

    /**
     * Getter for field cookies
     *
     * @return cookies
     */
    public Map<String, String> getCookies() {
        return cookies;
    }

    /**
     * Setter for field cookies
     *
     * @param cookies new value for field cookies
     */
    public void setCookies(Map<String, String> cookies) {
        this.cookies = cookies;
    }

    /**
     * Getter for field estat
     *
     * @return estat
     */
    public Map<String, String> getEstat() {
        return estat;
    }

    /**
     * Setter for field estat
     *
     * @param estat new value for field estat
     */
    public void setEstat(Map<String, String> estat) {
        this.estat = estat;
    }

    /**
     * Getter for field metrics
     *
     * @return metrics
     */
    public Map<String, String> getMetrics() {
        return metrics;
    }

    /**
     * Setter for field metrics
     *
     * @param metrics new value for field metrics
     */
    public void setMetrics(Map<String, String> metrics) {
        this.metrics = metrics;
    }

    /**
     * Return the serial
     *
     * @return
     */
    public String getSerial() {
        if (estat != null) {
            return estat.get(Hit.JSON_SERIAL);
        }
        return null;
    }

    @Override
    @JsonIgnore
    public Long getTs() {
        return HitUtils.extractTimestamp(qs.get(JSON_QS_TS));
    }

    /**
     * Return the hit timestamp parsed as a date
     */
    public String getHitDate(@Nonnull DateTimeFormatter formatter) {
        return formatter.format(getHitTimestamp().get());
    }

    /**
     * Format the current timestamp using the given DateTimeFormatter.
     * 
     * @return the formatted date in an {@link Optional} or absent() if timestamp is not defined.
     */
    public Optional<String> formatTimestamp(DateTimeFormatter dateFormatter) {
        if (timestamp == null) {
            return Optional.empty();
        }
        return Optional.of(dateFormatter.format(Instant.ofEpochMilli(timestamp)));
    }

    /**
     * Format the current qs.ts using the given DateTimeFormatter.
     *
     * @return the formatted date in an {@link Optional} or absent() if qs.ts is not defined.
     */
    public Optional<String> formatTs(DateTimeFormatter dateFormatter) {
        try {
            long ts = Long.parseLong(qs.get(Hit.JSON_QS_TS));
            return Optional.of(dateFormatter.format(Instant.ofEpochSecond(ts)));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonHit jsonHit = (JsonHit) o;
        return Objects.equal(uniqueId, jsonHit.uniqueId) &&
                Objects.equal(uri, jsonHit.uri) &&
                Objects.equal(remoteIp, jsonHit.remoteIp) &&
                Objects.equal(userAgent, jsonHit.userAgent) &&
                Objects.equal(timestamp, jsonHit.timestamp) &&
                Objects.equal(referer, jsonHit.referer) &&
                Objects.equal(host, jsonHit.host) &&
                Objects.equal(method, jsonHit.method) &&
                Objects.equal(status, jsonHit.status) &&
                Objects.equal(protocol, jsonHit.protocol) &&
                Objects.equal(contentLength, jsonHit.contentLength) &&
                Objects.equal(headers, jsonHit.headers) &&
                Objects.equal(qs, jsonHit.qs) &&
                Objects.equal(geoip, jsonHit.geoip) &&
                Objects.equal(cookies, jsonHit.cookies) &&
                Objects.equal(estat, jsonHit.estat) &&
                Objects.equal(metrics, jsonHit.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(uniqueId, uri, remoteIp, userAgent, timestamp, referer, host, method, status, protocol,
                contentLength, headers, qs, geoip, cookies, estat, metrics);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("uniqueId", uniqueId)
                .add("uri", uri)
                .add("remoteIp", remoteIp)
                .add("userAgent", userAgent)
                .add("timestamp", timestamp)
                .add("referer", referer)
                .add("host", host)
                .add("method", method)
                .add("status", status)
                .add("protocol", protocol)
                .add("contentLength", contentLength)
                .add("headers", headers)
                .add("qs", qs)
                .add("geoip", geoip)
                .add("cookies", cookies)
                .add("estat", estat)
                .add("metrics", metrics)
                .toString();
    }
}
