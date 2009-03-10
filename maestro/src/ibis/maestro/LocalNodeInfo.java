package ibis.maestro;

/**
 * Local information about a node.
 * 
 * @author Kees van Reeuwijk.
 */
class LocalNodeInfo {
    final boolean suspect;

    /** The number of currently running jobs of each type. */
    private final int currentJobs[];

    /** The number of in-flight (currently transmitted) jobs of each type. */
    private final int inFlightJobs[];

    /** The transmission time to the node for each type. */
    private final double transmissionTime[];

    /**
     * The predicted turnaround time to the master for each type of job. Used
     * for scheduling deadlines.
     */
    private final double predictedDuration[];

    LocalNodeInfo(boolean suspect, int inFlightJobs[], int currentJobs[], double transmissionTime[],
            double predictedDuration[]) {
        this.suspect = suspect;
        this.inFlightJobs = inFlightJobs;
        this.currentJobs = currentJobs;
        this.transmissionTime = transmissionTime;
        this.predictedDuration = predictedDuration;
    }

    int getCurrentJobs(JobType type) {
        return currentJobs[type.index];
    }

    int getInFlightJobs(JobType type) {
        return inFlightJobs[type.index];
    }

    double getTransmissionTime(JobType type) {
        return transmissionTime[type.index];
    }

    double getTransmissionTime(int ix) {
        return transmissionTime[ix];
    }

    /**
     * Given a job type, returns the predicted duration of the job on the
     * local node. (Used for scheduling deadlines.)
     * 
     * @param type
     *            The type of job we want the prediction for.
     * @return The predicted duration of the job in seconds.
     */
    double getPredictedDuration(JobType type) {
        return predictedDuration[type.index];
    }
}
