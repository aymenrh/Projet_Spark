package fr.mediametrie.internet.streaming.session;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.sql.Row;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Ordering;

//import fr.mediametrie.internet.libc.uamanager.UaManagerData;
import fr.mediametrie.internet.streaming.hit.Hit;
import fr.mediametrie.internet.streaming.hit.HitUtils;
import fr.mediametrie.internet.streaming.json.JsonParsersFactory;
import fr.mediametrie.internet.streaming.util.HashUtils;

/**
 * Represents a session with its attributes, mask and computed fields.
 * A Session is build from a list of {@link SessionHit} hits ordered by rank/ts/timestamp.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Session implements Hit, Serializable, Cloneable {

    public static final int DURATION_MAX_OFFSET = 120;

    /** interval/step in seconds when computing the nom_par_minute table. */
    public static final int PER_MINUTES_STEP_IN_SECONDS = 120;

    public String date;

    public String hash;
    public String cmsS1;
    public String cmsS2;
    public String cmsS3;
    public String cmsS4;
    public String cmsS5;
    public String cmsGR;
    public String cmsSN;
    public String dom;
    public String cmsVi;
    public String serial;
    public String cookie;
    public String ip;
    public String ua;

    public long beginTimestamp; // This timestamp is in seconds
    public long endTimestamp; // This timestamps is in seconds.

    public String countryCode;

    // Multi levels fields.
    public String ml1;
    public String ml2;
    public String ml3;
    public String ml4;
    public String ml5;
    public String ml6;
    public String ml7;
    public String ml8;
    public String ml9;
    public String ml10;
    public String ml11;
    public String msCid;
    public String msDm;
    public String msCh;
    public String miCh;

    public String brand;
    public String device;
    public String os;
    public String osVersion;

    // Fields used only during the reduce phase
    public List<SessionHit> hits = new ArrayList<>();
    public Long ts;
    public Long timestamp;
    public Integer rank;
    public String host;
    public String uid;

    // Fields used during the compute session (attributes, indicators, ...) phase
    public TreeSet<Range> playTimeRanges = new TreeSet<>();
    public int consumptionDuration;
    public int streamDuration;
    public long mask1;
    public long mask2;
    public long visitorId;
    public long visitId;
//    public UaManagerData.ResolveState uaResolveState; // Used to track the UA resolve states and count them in metrics.
    private long hitsCount;

    // Last time the session was updated
    public long lastUpdateTimestamp;

    public Session() {
        this.mask1 = 0;
        this.mask2 = 0;
    }

    /**
     * Return Map value if key exist, and dash by default.
     * @param map
     * @param key key to extract from map.
     * @return value or dash if key not exist.
     */
    private static String getMapValueWithDashByDefault(Map<String, String> map, String key) {
        if (map != null) {
            String value = map.get(key);
            if (value == null ) {
                return "-";
            } else {
                return value;
            }
        } else {
            return "-";
        }
    }

    /**
     * Initialize from a Dataframe {@link Row}
     *
     * @param row
     * @return {@link Session} from {@link Row}
     */
    public static Session fromRow(Row row) {
        Session session = new Session();



        return session;
    }

    // TODO this common code should be factorized with fromRow. Think about a good way to do it.
    /**
     * Build a {@link Session} from the given json. If an exception occurs during the json deserialization
     * or the object construction, null is returned.
     */
    @SuppressWarnings("unchecked")
    public static Session fromJsonHit(byte[] json) throws IOException {
        try {
            Map map = JsonParsersFactory.getJacksonJsonParser().readValue(json, Map.class);
            Session session = new Session();
            Map<String, String> qs = (Map<String, String>) map.get(Hit.JSON_QS);
            session.initQs(qs);
            session.initTimestamp(HitUtils.extractTimestamp(String.valueOf(map.get(JSON_TIMESTAMP))));
            session.initEstat((Map<String, String>) map.get(JSON_ESTAT));
            session.initCookies((Map<String, String>) map.get(JSON_COOKIES));
            session.initGeoip((Map<String, String>) map.get(JSON_GEOIP));

            session.ip = (String) map.get(JSON_IP);
            session.ua = (String) map.get(JSON_UA);

            session.host = (String) map.get(JSON_HOST);

            session.hits.add(new SessionHit(
                    (String) map.get(JSON_UNIQUE_ID),
                    session.getHitTimestampInSeconds().get(),
                    session.rank,
                    HitUtils.parseIntOrDefault(qs.get(JSON_QS_CMS_DU), 0),
                    HitUtils.parseIntOrDefault(qs.get(JSON_QS_CMS_PO), 0),
                    HitUtils.parseIntOrDefault(qs.get(JSON_QS_CMS_OP), 0),
                    HitUtils.parseIntOrDefault(qs.get(JSON_QS_CMS_PS), 0),
                    session.isCompactQuery()));

            return session;
        } catch (Exception e) {
            return null;
        }
    }

    public String getDate() {
        return date;
    }

    public Integer getRank() {
        return rank;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public Long getTs() {
        return ts;
    }

    public String getHash() {
        return hash;
    }

    public String getCmsS1() {
        return cmsS1;
    }

    public String getCmsS2() {
        return cmsS2;
    }

    public String getCmsS3() {
        return cmsS3;
    }

    public String getCmsS4() {
        return cmsS4;
    }

    public String getCmsS5() {
        return cmsS5;
    }

    public String getCmsGR() {
        return cmsGR;
    }

    public String getCmsSN() {
        return cmsSN;
    }

    public String getDom() {
        return dom;
    }

    public String getCmsVi() {
        return cmsVi;
    }

    public String getSerial() {
        return serial;
    }

    public String getUid() {
        return uid;
    }

    public String getCookie() {
        return cookie;
    }

    public String getIp() {
        return ip;
    }

    public String getUa() {
        return ua;
    }

    @Override
    public String getHost() {
        return host;
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public int getConsumptionDuration() {
        return consumptionDuration;
    }

    public int getStreamDuration() {
        return streamDuration;
    }

    public long getVisitorId() {
        return visitorId;
    }

    public long getVisitId() {
        return visitId;
    }

    public long getMask1() {
        return mask1;
    }

    public long getMask2() {
        return mask2;
    }

    public String getBrand() {
        return brand;
    }

    public String getDevice() {
        return device;
    }

    public String getOs() {
        return os;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    /**
     * @return hits, sorted and with no duplicate elements
     */
    public Collection<SessionHit> sortHitsAndRemoveDuplicates() {
        return new HashSet<>(hits).stream().sorted(SessionHit.HIT_COMPARATOR).collect(Collectors.toList());
    }

    /**
     * @return array of timestamps representing every minutes step when session is in play mode.
     */
    public long[] getMinutesWhenPlaying() {
        return playTimeRanges == null ? new long[0] :
                Range.flatRangesWithStep(playTimeRanges, PER_MINUTES_STEP_IN_SECONDS);
    }

    /**
     * @return the multilevels fields generated in json format.
     */
    public String generateMultilevelsJson() {
        return new Multilevels(this).toJson();
    }

    /**
     * Init all the session key related properties from the "timestamp" json value.
     */
    private void initTimestamp(Long value) {
        if (date == null && value != null) {
            date = HitUtils.dateFromEpochMillis(value);
        }
        timestamp = value;
    }

    /**
     * Init all the session key related properties from the "qs" json object.
     */
    private void initQs(Map<String, String> qs) {
        ts = HitUtils.extractTimestamp(qs.get(JSON_QS_TS));
        if (ts != null) {
            date = HitUtils.dateFromEpochSeconds(ts);
        }

        rank = HitUtils.parseIntOrDefault(qs.get(JSON_QS_CMS_RK), 0);

        hash = HitUtils.computeLevelHash(qs).toString();
        cmsS1 = getMapValueWithDashByDefault(qs, JSON_QS_CMS_S1);
        cmsS2 = getMapValueWithDashByDefault(qs, JSON_QS_CMS_S2);
        cmsS3 = getMapValueWithDashByDefault(qs, JSON_QS_CMS_S3);
        cmsS4 = qs.get(JSON_QS_CMS_S4);
        cmsS5 = qs.get(JSON_QS_CMS_S5);
        cmsGR = getMapValueWithDashByDefault(qs, JSON_QS_CMS_GR);
        cmsSN = getMapValueWithDashByDefault(qs, JSON_QS_CMS_SN);
        dom = getMapValueWithDashByDefault(qs, JSON_QS_DOM);
        cmsVi = qs.get(JSON_QS_CMSVI);

        uid = getMapValueWithDashByDefault(qs, JSON_QS_UID);

        // Multilevels fields
        ml1 = qs.get(JSON_QS_ML1);
        ml2 = qs.get(JSON_QS_ML2);
        ml3 = qs.get(JSON_QS_ML3);
        ml4 = qs.get(JSON_QS_ML4);
        ml5 = qs.get(JSON_QS_ML5);
        ml6 = qs.get(JSON_QS_ML6);
        ml7 = qs.get(JSON_QS_ML7);
        ml8 = qs.get(JSON_QS_ML8);
        ml9 = qs.get(JSON_QS_ML9);
        ml10 = qs.get(JSON_QS_ML10);
        ml11 = qs.get(JSON_QS_ML11);
        msCid = qs.get(JSON_QS_MSCID);
        msDm = qs.get(JSON_QS_MSDM);
        msCh = qs.get(JSON_QS_MSCH);
        miCh = qs.get(JSON_QS_MICH);
    }

    /**
     * Init all the session key related properties from the "estat" json object.
     */
    private void initEstat(Map<String, String> estat) {
        serial = estat.get(JSON_SERIAL);
    }

    /**
     * Init all the session key related properties from the "cookies" json object.
     */
    private void initCookies(Map<String, String> cookies) {
        cookie = getMapValueWithDashByDefault(cookies, JSON_COOKIES_E);
    }

    /**
     * Init all the session key related properties from the "geoip" json object.
     */
    private void initGeoip(Map<String, String> geoip) {
        if (geoip != null) {
            countryCode = geoip.get(JSON_GEOIP_COUNTRY_CODE);
        }
    }

    /**
     * Reduce two sessions in one, merging data
     *
     * @param session Session to merge with current session
     * @return Merged session
     */
    public Session reduce(Session session) {
        Session result = this;

        result.date = date;

        // Levels
        result.hash = hash;
        result.cmsS1 = cmsS1;
        result.cmsS2 = cmsS2;
        result.cmsS3 = cmsS3;
        result.cmsS4 = cmsS4;
        result.cmsS5 = cmsS5;
        result.cmsGR = cmsGR;
        result.cmsSN = cmsSN;
        result.dom = dom;
        result.cmsVi = cmsVi;

        result.serial = serial;

        Session mostRecent = SESSION_ORDERING.max(this, session);

        // Attributes
        result.uid = mostRecent.uid;
        result.cookie = mostRecent.cookie;
        result.ip = mostRecent.ip;
        result.ua = mostRecent.ua;
        result.countryCode = mostRecent.countryCode;

        Session mostAncient = SESSION_ORDERING.min(this, session);

        // Multi levels fields.
        result.ml1 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml1) ? mostRecent.ml1 : mostAncient.ml1;
        result.ml2 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml2) ? mostRecent.ml2 : mostAncient.ml2;
        result.ml3 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml3) ? mostRecent.ml3 : mostAncient.ml3;
        result.ml4 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml4) ? mostRecent.ml4 : mostAncient.ml4;
        result.ml5 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml5) ? mostRecent.ml5 : mostAncient.ml5;
        result.ml6 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml6) ? mostRecent.ml6 : mostAncient.ml6;
        result.ml7 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml7) ? mostRecent.ml7 : mostAncient.ml7;
        result.ml8 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml8) ? mostRecent.ml8 : mostAncient.ml8;
        result.ml9 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml9) ? mostRecent.ml9 : mostAncient.ml9;
        result.ml10 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml10) ? mostRecent.ml10 : mostAncient.ml10;
        result.ml11 = HitUtils.isMultiLevelFieldRelevant(mostRecent.ml11) ? mostRecent.ml11 : mostAncient.ml11;
        result.msCid = HitUtils.isMultiLevelFieldRelevant(mostRecent.msCid) ? mostRecent.msCid : mostAncient.msCid;
        result.msDm = HitUtils.isMultiLevelFieldRelevant(mostRecent.msDm) ? mostRecent.msDm : mostAncient.msDm;
        result.msCh = HitUtils.isMultiLevelFieldRelevant(mostRecent.msCh) ? mostRecent.msCh : mostAncient.msCh;
        result.miCh = HitUtils.isMultiLevelFieldRelevant(mostRecent.miCh) ? mostRecent.miCh : mostAncient.miCh;

        // Keep most recent values
        if (ts != null) {
            if (session.ts != null) {
                result.ts = Math.max(ts, session.ts);
            }
        } else if (session.ts != null) {
            result.ts = session.ts;
        }
        result.timestamp = Math.max(timestamp, session.timestamp);
        result.rank = Math.max(rank, session.rank);

        // Merge hits
        if (result.hits == null) {
            result.hits = new ArrayList<>(session.hits);
        } else {
            result.hits.addAll(session.hits);
        }
        result.hitsCount = result.hits.size();

        return result;
    }

    public Session compute(SessionComputeOptions options) {
        return compute(options, true);
    }

    public Session computeRealtime(SessionComputeOptions options) {
        return compute(options, false);
    }

    /**
     * Compute session after reduce operation
     *
     * @param options
     * @param cleanHitsAfterCompute if true, the associated hits list is cleaned.
     *            The hits list is cleaned suring process batch to clear memory.
     *            Hits are not cleaned during realtime as sessions need to be recomputed
     */
    public Session compute(SessionComputeOptions options, boolean cleanHitsAfterCompute) {
        // Reset indicators and fields before compute to avoid bad count if called multiple times (realtime)
        consumptionDuration = 0;
        streamDuration = 0;

        Collection<SessionHit> sortedHits = sortHitsAndRemoveDuplicates();
        computeAttributes(sortedHits);
        computeVisitorId();
        computeIndicators(sortedHits, options.masksCheckTolerance, options.durationCheckTolerance);
        // Some cleaning is needed after compute on fields use only for the reduce phase.
        if (cleanHitsAfterCompute) {
            this.hits = null;
        }
        return this;
    }

    /**
     * Update all atributes after reduce operation
     * @param sortedHits
     */
    public void computeAttributes(Collection<SessionHit> sortedHits) {
        if (!Strings.isNullOrEmpty(uid) && !"-".equals(uid)) {
            this.cookie = uid;
        }
        for (SessionHit hit : sortedHits) {
            updateDuration(hit);
            updateTimestamps(hit);
        }
    }

    /**
     * Updates the duration only if greater thant the current one.
     */
    private void updateDuration(SessionHit hit) {
        this.streamDuration = hit.duration > this.streamDuration ? hit.duration : streamDuration;
    }

    /**
     * Update the begin_ts and end_ts with the given hit timestamp.
     * The begin_ts is updated only if the hit ts is before the current value.
     * The end_ts is updated only if the hit ts is after the current value.
     */
    private void updateTimestamps(SessionHit hit) {
        beginTimestamp = beginTimestamp == 0 || hit.tsInSeconds < beginTimestamp ? hit.tsInSeconds : beginTimestamp;
        endTimestamp = endTimestamp == 0 || hit.tsInSeconds > endTimestamp ? hit.tsInSeconds : endTimestamp;
    }

    /**
     * Compute the visitor Id for this session. If the cookie is set, visitor id is based on the cookie, otherwise it is
     * based on the IP/UA.
     */
    public void computeVisitorId() {
        if (cookie != null && HitUtils.isCookieRelevant(cookie.trim())) {
            visitorId = HashUtils.Murmur3AsLong(date, cookie);
        } else {
            visitorId = HashUtils.Murmur3AsLong(date, ip, ua);
        }
    }

    /**
     * Compute indicators for this session.
     *
     * @param sortedHits
     * @param masksCheckTolerance
     * @param durationCheckTolerance
     */
    public void computeIndicators(Collection<SessionHit> sortedHits, int masksCheckTolerance,
            int durationCheckTolerance) {
        ArrayList<Long> playHitsTimestamps = new ArrayList<>();
        long lastTimestamp = 0;
        boolean isPlaying = false;
        for (SessionHit hit : sortedHits) {
            updateMask(hit, lastTimestamp, isPlaying, masksCheckTolerance);
            updateDuration(hit, lastTimestamp, isPlaying, durationCheckTolerance);
            // Update at the end of loop to save state for the last hit.
            lastTimestamp = hit.tsInSeconds;
            isPlaying = hit.isPlaying();
            if (isPlaying) {
                playHitsTimestamps.add(lastTimestamp);
            }
        }
        this.playTimeRanges = Range.build(playHitsTimestamps, PER_MINUTES_STEP_IN_SECONDS);
    }

    /**
     * Updates the session mask 1 and 2 with the given hit.
     */
    public void updateMask(SessionHit hit, long lastTimestamp, boolean isPlaying,
            int masksCheckTolerance) {
        if (HitMask.isValidToAddMaskToSession(hit, isPlaying, lastTimestamp, masksCheckTolerance)) {
            HitMask hitMask = new HitMask();
            hitMask.computeHitMask(hit.oldPosition, hit.position, this.streamDuration);

            mask1 |= hitMask.getHitMask1() & HitMask.CLEAN_MASK;
            mask2 |= hitMask.getHitMask2() & HitMask.CLEAN_MASK;
        }
    }

    /**
     * Update consumptionDuration
     *
     * @param hit
     * @param lastTimestamp
     * @param isPlaying
     * @param durationCheckTolerance
     */
    public void updateDuration(SessionHit hit, long lastTimestamp, boolean isPlaying,
            int durationCheckTolerance) {
        int offset_position = hit.position - hit.oldPosition;

        // First we check for compact query. Very simple algorithm
        if (hit.isCompactQuery()) {
            consumptionDuration += offset_position;
        } else if (lastTimestamp == 0) { // Last timestamp==0 means First hit
            if (offset_position >= 0
                    && offset_position <= DURATION_MAX_OFFSET + durationCheckTolerance) {
                consumptionDuration += offset_position;
            }
        } else if (isPlaying) { // Subsequent hits (ignore non playing state
            long offset_timestamp = hit.tsInSeconds - lastTimestamp;
            if (offset_timestamp > 0 && offset_timestamp <= DURATION_MAX_OFFSET) {

                if (offset_position >= 1 && offset_position <= DURATION_MAX_OFFSET - durationCheckTolerance
                        && offset_position < offset_timestamp) {
                    consumptionDuration += offset_position;
                } else {
                    consumptionDuration += offset_timestamp;
                }
            }
        }
    }

    /**
     * Assign visit ids to the given sessions.
     * The provided sessions should be associated to the same visitor id.
     * The folowing algorithm is applied: all sessions are sorted by ts_begin asc, then
     * a new session id is assigned when a inactivity time occurs between 2 sessions.
     *
     * @param sessions                A {@link Session} list related to the same visitor.
     * @param inactivityTimeInSeconds how much time in seconds between 2 sessions to generate a new session id.
     */
    @SuppressWarnings("ConstantConditions")
    public static void assignVisitIds(Iterable<Session> sessions, int inactivityTimeInSeconds) {
        VisitProcessState state = new VisitProcessState(inactivityTimeInSeconds);
        StreamSupport.stream(sessions.spliterator(), false)
                .sorted(VISIT_ID_CALCULATION_COMPARATOR)
                .forEach(session -> session.visitId = state.update(session));
    }

    /**
     * Compare 2 sessions based on rank and timestamp
     */
    private static final Comparator<Session> SESSION_COMPARATOR = (session1, session2) -> {
        int myComp;

        /* check rank */
        if ((myComp = Integer.compare(session1.rank, session2.rank)) != 0)
            return myComp;

        /* check timestamp */
        return Long.compare(session1.getHitTimestampInSeconds().get(), session2.getHitTimestampInSeconds().get());
    };

    private static final Ordering<Session> SESSION_ORDERING = Ordering.from(SESSION_COMPARATOR);

    /**
     * Comparator to sort {@link Session} by begin ts.
     */
    private static final Comparator<Session> VISIT_ID_CALCULATION_COMPARATOR = (sess1, sess2) -> {
        if (sess1.visitorId == sess2.visitorId) {
            return Long.compare(sess1.beginTimestamp, sess2.beginTimestamp);
        } else {
            return Long.compare(sess1.visitorId, sess2.visitorId);
        }
    };

    public Long getHitsCount() {
        return hitsCount;
    }

    public void addHit(SessionHit sessionHit) {
        hits.add(sessionHit);
    }

    /**
     * Class to serialize multilevels as json string.
     */
    // nosonar
    @SuppressWarnings("unused")
    private static class Multilevels {
        private static final ObjectMapper mapper = JsonParsersFactory.getMultilevelMapper();

        // fields must be in uppercase to be compliant with lecacy.
        public String ml1;
        public String ml2;
        public String ml3;
        public String ml4;
        public String ml5;
        public String ml6;
        public String ml7;
        public String ml8;
        public String ml9;
        public String ml10;
        public String ml11;
        public String mscid;
        public String msdm;
        public String msch;
        public String mich;

        public Multilevels(Session session) {
            this.ml1 = session.ml1;
            this.ml2 = session.ml2;
            this.ml3 = session.ml3;
            this.ml4 = session.ml4;
            this.ml5 = session.ml5;
            this.ml6 = session.ml6;
            this.ml7 = session.ml7;
            this.ml8 = session.ml8;
            this.ml9 = session.ml9;
            this.ml10 = session.ml10;
            this.ml11 = session.ml11;
            this.mscid = session.msCid;
            this.msdm = session.msDm;
            this.msch = session.msCh;
            this.mich = session.miCh;
        }

        public String toJson() {
            try {
                return mapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                return "";
            }
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
        Session session = (Session) o;
        return beginTimestamp == session.beginTimestamp &&
                endTimestamp == session.endTimestamp &&
                consumptionDuration == session.consumptionDuration &&
                streamDuration == session.streamDuration &&
                visitorId == session.visitorId &&
                visitId == session.visitId &&
                mask1 == session.mask1 &&
                mask2 == session.mask2 &&
                Objects.equal(date, session.date) &&
                Objects.equal(rank, session.rank) &&
                Objects.equal(timestamp, session.timestamp) &&
                Objects.equal(ts, session.ts) &&
                Objects.equal(hash, session.hash) &&
                Objects.equal(cmsS1, session.cmsS1) &&
                Objects.equal(cmsS2, session.cmsS2) &&
                Objects.equal(cmsS3, session.cmsS3) &&
                Objects.equal(cmsS4, session.cmsS4) &&
                Objects.equal(cmsS5, session.cmsS5) &&
                Objects.equal(cmsGR, session.cmsGR) &&
                Objects.equal(cmsSN, session.cmsSN) &&
                Objects.equal(dom, session.dom) &&
                Objects.equal(cmsVi, session.cmsVi) &&
                Objects.equal(serial, session.serial) &&
                Objects.equal(uid, session.uid) &&
                Objects.equal(cookie, session.cookie) &&
                Objects.equal(ip, session.ip) &&
                Objects.equal(ua, session.ua) &&
                Objects.equal(host, session.host) &&
                Objects.equal(countryCode, session.countryCode) &&
                Objects.equal(ml1, session.ml1) &&
                Objects.equal(ml2, session.ml2) &&
                Objects.equal(ml3, session.ml3) &&
                Objects.equal(ml4, session.ml4) &&
                Objects.equal(ml5, session.ml5) &&
                Objects.equal(ml6, session.ml6) &&
                Objects.equal(ml7, session.ml7) &&
                Objects.equal(ml8, session.ml8) &&
                Objects.equal(ml9, session.ml9) &&
                Objects.equal(ml10, session.ml10) &&
                Objects.equal(ml11, session.ml11) &&
                Objects.equal(msCid, session.msCid) &&
                Objects.equal(msDm, session.msDm) &&
                Objects.equal(msCh, session.msCh) &&
                Objects.equal(miCh, session.miCh) &&
                Objects.equal(brand, session.brand) &&
                Objects.equal(device, session.device) &&
                Objects.equal(os, session.os) &&
                Objects.equal(osVersion, session.osVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(date, ts, timestamp, rank, hash, cmsS1, cmsS2, cmsS3, cmsS4, cmsS5, cmsGR, cmsSN, dom,
                cmsVi, serial, uid, cookie, ip, ua, host, beginTimestamp, endTimestamp, countryCode, ml1, ml2, ml3, ml4,
                ml5, ml6, ml7, ml8, ml9, ml10, ml11, msCid, msDm, msCh, miCh, consumptionDuration, streamDuration,
                visitorId, visitId, mask1, mask2, brand, device, os, osVersion);
    }

    @Override
    public Session clone() throws CloneNotSupportedException {
        return (Session) super.clone();
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
                .add("serial", serial)
                .add("cmsVi", cmsVi)
                .add("hits", hits)
                .toString();
    }
}

