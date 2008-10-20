package ibis.maestro;

/**
 * Local information about the nodes. 
 *
 * @author Kees van Reeuwijk.
 */
class LocalNodeInfo
{
    final boolean suspect;
    
    /** The number of currently running tasks of each type. */
    final int currentTasks[];
    
    /** The allowance for each type. */
    final int allowance[];
    
    /** The transmission time to the node for each type. */
    final long transmissionTime[];
    
    /** The predicted turnaround time to the master for each type of task. Used for scheduling deadlines. */
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

    long getTransmissionTime( int ix )
    {
	return transmissionTime[ix];
    }

    /** Given a task type, returns the predicted duration of the task
     * on the local node. (Used for scheduling deadlines.)
     * @param type The type of task we want the prediction for.
     * @return The predicted duration of the task in nanoseconds.
     */
    long getPredictedDuration( TaskType type )
    {
        return predictedDuration[type.index];
    }
}
