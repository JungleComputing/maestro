/**
 * 
 */
package ibis.maestro;

class LocalNodeInfo {
    final int currentJobs;
    final double transmissionTime;
    final double predictedDuration;

    /**
     * @param currentJobs
     * @param transmissionTime
     * @param predictedDuration
     */
    LocalNodeInfo(int currentJobs, double transmissionTime,
            double predictedDuration) {
        this.currentJobs = currentJobs;
        this.transmissionTime = transmissionTime;
        this.predictedDuration = predictedDuration;
    }
    
    public String toString()
    {
    	return "(currentJobs=" + currentJobs
    	  + ",transmissionTime=" + Utils.formatSeconds(transmissionTime) +
    	  ",predictedDuration=" + Utils.formatSeconds(predictedDuration) + ")";
    	 
    }
}
