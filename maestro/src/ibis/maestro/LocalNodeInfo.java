package ibis.maestro;

/**
 * FIXME.
 *
 * @author Kees van Reeuwijk.
 */
public class LocalNodeInfo
{
    final boolean suspect;
    final int currentTasks[];
    final long transmissionTime[];
    final long predictedDuration[];

    
    LocalNodeInfo( boolean suspect, int[] currentTasks, long[] transmissionTime, long predictedDuration[] )
    {
        this.suspect = suspect;
        this.currentTasks = currentTasks;
        this.transmissionTime = transmissionTime;
        this.predictedDuration = predictedDuration;
    }

    int getCurrentTasks( TaskType type )
    {
        return currentTasks[type.index];
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
