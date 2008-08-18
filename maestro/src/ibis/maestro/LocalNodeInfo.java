package ibis.maestro;

/**
 * Local information about the nodes. 
 *
 * @author Kees van Reeuwijk.
 */
class LocalNodeInfo
{
    final boolean suspect;
    final int currentTasks[];
    final int allowance[];
    final long transmissionTime[];
    final long predictedDuration[];

    
    LocalNodeInfo( boolean suspect, int[] currentTasks, int allowance[], long[] transmissionTime, long predictedDuration[] )
    {
        this.suspect = suspect;
        this.currentTasks = currentTasks;
        this.allowance = allowance;
        this.transmissionTime = transmissionTime;
        this.predictedDuration = predictedDuration;
    }

    int getCurrentTasks( TaskType type )
    {
        return currentTasks[type.index];
    }

    int getAllowance( TaskType type )
    {
        return allowance[type.index];
    }

    long getTransmissionTime( TaskType type )
    {
        return transmissionTime[type.index];
    }

    /** Given a task type, returns the predicted duration of the task
     * on the local node.
     * @param type The type of task we want the prediction for.
     * @return The predicted duration of the task in nanoseconds.
     */
    long getPredictedDuration( TaskType type )
    {
        return predictedDuration[type.index];
    }

    long getTransmissionTime(int ix)
    {
	return transmissionTime[ix];
    }
}
