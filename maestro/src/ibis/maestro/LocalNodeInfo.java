/**
 * 
 */
package ibis.maestro;

class LocalNodeInfo {
    final int inFlightJobs;
    final int currentJobs;
    final double transmissionTime;
    final double predictedDuration;

    /**
     * @param currentJobs
     * @param transmissionTime
     * @param predictedDuration
     */
    LocalNodeInfo(int inFlightJobs,int currentJobs, double transmissionTime,
            double predictedDuration) {
        this.inFlightJobs = inFlightJobs;
        this.currentJobs = currentJobs;
        this.transmissionTime = transmissionTime;
        this.predictedDuration = predictedDuration;
    }
    
    public String toString()
    {
    	return "(inFlight=" + inFlightJobs + ",currentJobs="
    	  + ",transmissionTime=" + Utils.formatSeconds(transmissionTime) +
    	  ",predictedDuration=" + Utils.formatSeconds(predictedDuration) + ")";
    	 
    }
}