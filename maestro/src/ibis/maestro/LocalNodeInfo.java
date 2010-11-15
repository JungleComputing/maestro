/**
 * 
 */
package ibis.maestro;

class LocalNodeInfo {
    final int currentJobs;
    final TimeEstimate transmissionTime;
    final TimeEstimate predictedDuration;

    /**
     * @param currentJobs
     * @param transmissionTime
     * @param predictedDuration
     */
    LocalNodeInfo(int currentJobs, TimeEstimate transmissionTime,
            TimeEstimate predictedDuration) {
        this.currentJobs = currentJobs;
        this.transmissionTime = transmissionTime;
        this.predictedDuration = predictedDuration;
    }

    @Override
    public String toString() {
        return "(currentJobs=" + currentJobs + ",transmissionTime="
                + Utils.formatSeconds(transmissionTime) + ",predictedDuration="
                + Utils.formatSeconds(predictedDuration) + ")";

    }
}
