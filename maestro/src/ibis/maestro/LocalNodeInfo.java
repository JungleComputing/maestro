/**
 * 
 */
package ibis.maestro;

import ibis.steel.Estimate;

class LocalNodeInfo {
    final int currentJobs;
    final Estimate transmissionTime;
    final Estimate predictedDuration;

    /**
     * @param currentJobs
     * @param transmissionTime2
     * @param predictedDuration
     */
    LocalNodeInfo(final int currentJobs, final Estimate transmissionTime2,
            final Estimate predictedDuration) {
        this.currentJobs = currentJobs;
        this.transmissionTime = transmissionTime2;
        this.predictedDuration = predictedDuration;
    }

    @Override
    public String toString() {
        return "(currentJobs=" + currentJobs + ",transmissionTime="
                + transmissionTime.format() + ",predictedDuration="
                + predictedDuration.format() + ")";

    }
}
