package fr.mediametrie.internet.streaming.session;

import java.io.Serializable;

import com.google.common.base.Objects;

/**
 * {@link SessionKey} are neede to group incoming hits by sessions.
 * <p>
 * {@link SessionKey} are created from the raw json hit.
 * </p>
 * <p>
 * The level hash is computer using HashMurmur3 128 thru guava library.
 * </p>
 */
public class SessionKey implements Serializable {

    public String date;

    public String serial;

    public String cmsVi;

    public String levelHash;

    /**
     * 
     */
    public static SessionKey fromSession(Session session) {
        SessionKey key = new SessionKey();
        key.serial = session.serial;
        key.date = session.date;
        key.cmsVi = session.cmsVi;
        key.levelHash = session.hash;
        return key;
    }

    /**
     * Same as {@link #fromSession(Session)} but does not include the date in the key
     * 
     * @param session The session to compute the of
     * @return The session key
     */
    public static SessionKey fromSessionRt(Session session) {
        SessionKey key = new SessionKey();
        key.serial = session.serial;
        key.cmsVi = session.cmsVi;
        key.levelHash = session.hash;
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SessionKey that = (SessionKey) o;

        return Objects.equal(this.date, that.date) &&
                Objects.equal(this.serial, that.serial) &&
                Objects.equal(this.cmsVi, that.cmsVi) &&
                Objects.equal(this.levelHash, that.levelHash);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(date, serial, cmsVi, levelHash);
    }
}
