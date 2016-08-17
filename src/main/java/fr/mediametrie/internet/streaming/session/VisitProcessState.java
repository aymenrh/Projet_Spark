package fr.mediametrie.internet.streaming.session;

import fr.mediametrie.internet.streaming.util.IDGenerator;

/**
 * Class used during visit processing to allow global usage of primitives in a lambda.
 */
public class VisitProcessState {
    static final IDGenerator idGenerator = new IDGenerator();

    private int inactivityTimeInSeconds;

    public long currentVisitId = idGenerator.next();
    public long previousEndTimestamp = 0;
    public long currentVisitorId = 0;

    public VisitProcessState(int inactivityTimeInSeconds) {
        this.inactivityTimeInSeconds = inactivityTimeInSeconds;
    }

    public void updateEndTimestampToMoreRecent(long ts) {
        this.previousEndTimestamp = Math.max(ts, this.previousEndTimestamp);
    }

    /**
     * Update the current visit id with a new generated one and return it.
     */
    public long nextVisitId() {
        this.currentVisitId = idGenerator.next();
        return this.currentVisitId;
    }

    /**
     * Update the current state depending of the given session.
     * A new visit id is generated when the given session is a new visitor.
     * A new visit id is generated when more than {@link #inactivityTimeInSeconds} seconds occurs from the
     * previous non empty (ts_vis != 0) session from the current visitor.
     */
    public long update(Session session) {
        if (session.visitorId != currentVisitorId) {
            nextVisitId();
            currentVisitorId = session.visitorId;
            previousEndTimestamp = session.consumptionDuration > 0 ? session.endTimestamp : 0;
        } else if (session.consumptionDuration > 0) {
            if (previousEndTimestamp > 0 &&
                    session.beginTimestamp - previousEndTimestamp > inactivityTimeInSeconds) {
                nextVisitId();
            }
            updateEndTimestampToMoreRecent(session.endTimestamp);
        }
        return currentVisitId;
    }
}
