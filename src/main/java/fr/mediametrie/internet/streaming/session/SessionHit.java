package fr.mediametrie.internet.streaming.session;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Comparator;

public class SessionHit implements Serializable {

    public static Comparator<SessionHit> HIT_COMPARATOR = (hit1, hit2) -> {
        int myComp;

        /* check rank */
        if ((myComp = Integer.compare(hit1.cmsRk, hit2.cmsRk)) != 0)
            return myComp;

        /* check timestamp */
        return Long.compare(hit1.tsInSeconds, hit2.tsInSeconds);
    };


    public String uniqueId;
    public long tsInSeconds;
    public int cmsRk;
    public int position;
    public int oldPosition;
    public int duration;
    public int playState;
    public boolean isCompactQuery;

    public SessionHit() {
        // For tests
    }

    public SessionHit(String uniqueId, long tsInSeconds, int cmsRk, int duration, int position, int oldPosition,
            int playState,
            boolean isCompactQuery) {
        this.uniqueId = uniqueId;
        this.tsInSeconds = tsInSeconds;
        this.cmsRk = cmsRk;
        this.duration = duration;
        this.position = position;
        this.oldPosition = oldPosition;
        this.playState = playState;
        this.isCompactQuery = isCompactQuery;
    }

    public boolean isPlaying() {
        return playState == 5 || playState == 4;
    }

    public boolean isCompactQuery() {
        return isCompactQuery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionHit that = (SessionHit) o;
        return tsInSeconds == that.tsInSeconds &&
                cmsRk == that.cmsRk &&
                position == that.position &&
                oldPosition == that.oldPosition &&
                duration == that.duration &&
                playState == that.playState &&
                isCompactQuery == that.isCompactQuery &&
                Objects.equal(uniqueId, that.uniqueId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(uniqueId, tsInSeconds, cmsRk, position, oldPosition, duration, playState,
                isCompactQuery);
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
                .add("cmsRk", cmsRk)
                .add("oldPosition", oldPosition)
                .add("position", position)
                .add("playState", playState)
                .toString();
    }
}
