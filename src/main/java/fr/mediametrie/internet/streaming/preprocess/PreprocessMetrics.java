package fr.mediametrie.internet.streaming.preprocess;

import java.io.Serializable;

import com.google.common.base.Objects;

/**
 * Preprocessing metrics used to generate run report
 */
public class PreprocessMetrics implements Serializable {

    /**
     * Total number of hits.
     */
    int incomingHitCount = 0;

    /**
     * Number of valid hits.
     */
    int validHitCount = 0;

    /**
     * Number of invalid hits because of the JSon formatting.
     */
    int invalidHitWithBadJsonCount = 0;

    /**
     * Number of valid hits but having not Geoloc information.
     */
    int validHitWithoutGeolocCount = 0;

    /**
     * Number of valid hits but having not been URL decoded.
     */
    int validHitWithoutUrlDecodeCount = 0;

    public PreprocessMetrics() {
    }

    /**
     * Merge two PreprocessMetrics, summing values.
     *
     * @param metrics1 1st PreprocessMetrics to merge
     * @param metrics2 2nd PreprocessMetrics to merge
     * @return the merged PreprocessMetrics
     */
    public static PreprocessMetrics reduce(PreprocessMetrics metrics1, PreprocessMetrics metrics2) {
        PreprocessMetrics result = new PreprocessMetrics();
        result.incomingHitCount = metrics1.incomingHitCount + metrics2.incomingHitCount;
        result.validHitCount = metrics1.validHitCount + metrics2.validHitCount;
        result.invalidHitWithBadJsonCount = metrics1.invalidHitWithBadJsonCount + metrics2.invalidHitWithBadJsonCount;
        result.validHitWithoutGeolocCount = metrics1.validHitWithoutGeolocCount + metrics2.validHitWithoutGeolocCount;
        result.validHitWithoutUrlDecodeCount = metrics1.validHitWithoutUrlDecodeCount + metrics2.validHitWithoutUrlDecodeCount;
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("incomingHitCount", incomingHitCount)
                .add("validHitCount", validHitCount)
                .add("invalidHitWithBadJsonCount", invalidHitWithBadJsonCount)
                .add("validHitWithoutGeolocCount", validHitWithoutGeolocCount)
                .add("validHitWithoutUrlDecodeCount", validHitWithoutUrlDecodeCount)
                .toString();
    }
}
