package ibis.maestro;

import java.util.Arrays;

/**
 * Local information about a node.
 * 
 * @author Kees van Reeuwijk.
 */
class LocalNodeInfoList {
    final boolean suspect;

    private final LocalNodeInfo infoPerType[];

    LocalNodeInfoList(boolean suspect, LocalNodeInfo[] infoPerType) {
        this.suspect = suspect;
        this.infoPerType = infoPerType;
    }

    /**
     * Given a job type, returns local performance info for that job type.
     * 
     * @param type
     *            The type of job we want the info for.
     * @return The local performance info.
     */
    LocalNodeInfo getLocalNodeInfo(JobType type)
    {
        return infoPerType[type.index];
    }

    double getTransmissionTime(int ix) {
        return infoPerType[ix].transmissionTime;
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
        return infoPerType[type.index].predictedDuration;
    }
    
    public String toString()
    {
    	return Arrays.deepToString(infoPerType);
    }
}
