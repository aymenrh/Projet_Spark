package fr.mediametrie.internet.streaming.session;

import java.io.Serializable;

/**
 * Options and magic numbers used to compute a session.
 */
public class SessionComputeOptions implements Serializable {

    public int masksCheckTolerance;

    public int durationCheckTolerance;

}
