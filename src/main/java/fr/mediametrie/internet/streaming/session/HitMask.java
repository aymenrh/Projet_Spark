package fr.mediametrie.internet.streaming.session;

public class HitMask {

    // Ensures that only the necessary 50 bits of the 64, can be different from 0
    public static final long CLEAN_MASK = Long.parseLong("0003FFFFFFFFFFFF", 16);

    // Mask for the first 50 bits
    private long hitMask1;
    // Mask for the last 50 bits
    private long hitMask2;

    public HitMask() {
        this.hitMask2 = 0;
        this.hitMask1 = 0;
    }

    public long getHitMask1() {
        return hitMask1;
    }

    public long getHitMask2() {
        return hitMask2;
    }

    /**
     * Checks the informations hit validity to calculate masks.
     * <p>
     * FIXME : The test checking that the position is smaller than the duration has been temporarily commented
     * out. This is decision of the client as computed results are otherwise too different from the current
     * production results. This check is meant to come back at some point, because it really makes sense, we
     * just don't know when.
     * </p>
     *
     * @param oldPosition Hit old position (cmsop) information.
     * @param position    Hit position (cmspo) information
     * @param duration    Flow duration.
     * @return True if hit informations are valid.
     */
    private boolean isThatSessionHitIsValidForMaskCalculation(int oldPosition, int position, int duration) {
        long positionOffset = position - oldPosition;
        return duration > 0 && positionOffset > 0 && position >= 0 && oldPosition >= 0; // && position <= duration;
    }

    /**
     * Compute current session hit mask 1 and mask 2
     *
     * @param oldPosition Hit old position (cmsop) information.
     * @param position    Hit position (cmspo) information
     * @param duration    Flow duration.
     * @see <a href="https://mediametrie.atlassian.net/wiki/pages/viewpage.action?pageId=69141033">Complétion (masques)</a>
     */
    public void computeHitMask(int oldPosition, int position, int duration) {
        long oldPositionMask1 = 0L;
        long oldPositionMask2 = 0L;

        long positionMask1 = 0L;
        long positionMask2 = 0L;

        if (isThatSessionHitIsValidForMaskCalculation(oldPosition, position, duration)) {
            int percentPosition = (int) (((float) position / (float) duration) * 100);
            int percentOldPosition = (int) (((float) oldPosition / (float) duration) * 100);

            // limit percentPosition to 100% maximum
            if (percentPosition > 100) {
                percentPosition = 100;
            }

            if (percentOldPosition > 50) {
                // we use Math.ceil to reproduce SQL aggregation that rounds e.g. -0.75 to 0 where Java cast rounds
                // -0.75 to -1.0
                positionMask2 = (long) Math.ceil(Math.pow(2, (50 - (percentOldPosition - 50))) - 1);
            } else {
                oldPositionMask2 = (long) Math.pow(2, (50 - percentOldPosition)) - 1;
                positionMask2 = (long) Math.pow(2, 50) - 1;
            }

            if (percentPosition > 50) {
                // we use Math.ceil to reproduce SQL aggregation that rounds e.g. -0.75 to 0 where Java cast rounds
                // -0.75 to -1.0
                positionMask1 = (long) Math.ceil(Math.pow(2, (50 - (percentPosition - 50))) - 1);
            } else {
                oldPositionMask1 = (long) Math.pow(2, (50 - percentPosition)) - 1;
                positionMask1 = (long) Math.pow(2, 50) - 1;
            }

            hitMask1 = (oldPositionMask2 ^ oldPositionMask1) & CLEAN_MASK;
            hitMask2 = (positionMask2 ^ positionMask1) & CLEAN_MASK;
        }
    }

    /**
     * Check informations to verify if we can apply hitMask to the session.
     *
     * @param hit current Hit
     * @param isPlaying The session is playing currently ?
     * @param lastTimestamp Last timestamp.
     * @param masksCheckTolerance
     * @return True if we can apply hitMask to the session.
     */
    public static boolean isValidToAddMaskToSession(SessionHit hit, boolean isPlaying, long lastTimestamp,
            int masksCheckTolerance) {
        /* For compact hit, we don't check anything */
        if (hit.isCompactQuery()) {
            return true;
        }

        long timestampOffset = 0;
        if (lastTimestamp > 0) {
            timestampOffset = hit.tsInSeconds - lastTimestamp;
        }
        long positionOffset = hit.position - hit.oldPosition;

        /* 3 it's magic number to define a tolerance of hit reception. */
        if (isPlaying && (positionOffset <= (timestampOffset + masksCheckTolerance))) {
            return true;
        }

        return false;
    }
}
