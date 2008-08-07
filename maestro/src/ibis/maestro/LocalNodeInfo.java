package ibis.maestro;

/**
 * Local information about the nodes. 
 *
 * @author Kees van Reeuwijk.
 */
public class LocalNodeInfo
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

    /** FIXME.
     * @param type
     * @return
     */
    long getPredictedDuration( TaskType type )
    {
        return predictedDuration[type.index];
    }
}
