package ibis.maestro;

import java.io.PrintStream;
import java.util.ArrayList;

/**
 * Information about all tasks.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInfoList
{
    private final ArrayList<TaskInfo> taskInfoList = new ArrayList<TaskInfo>();

    /**
     * Returns the TaskInfoOnMaster instance for the given task type. If
     * necessary, extend the taskInfoList to cover this type. If necessary,
     * create a new class instance for this type.
     * @param type The type we want info for.
     * @return The info structure for the given type.
     */
    TaskInfo getTaskInfo( TaskType type )
    {
        int ix = type.index;
        while( ix+1>taskInfoList.size() ) {
            taskInfoList.add( null );
        }
        TaskInfo res = taskInfoList.get( ix );
        if( res == null ) {
            res = new TaskInfo( type );
            taskInfoList.set( ix, res );
        }
        return res;
    }
    
    private void registerLocalTask( TaskType type, JobList jobs )
    {
        Task task = jobs.getTask( type );
        if( task instanceof TaskExecutionTimeEstimator ) {
            TaskExecutionTimeEstimator estimator = (TaskExecutionTimeEstimator) task;
            TaskInfo info = getTaskInfo( type );
            info.setInitialComputeTimeEstimate( estimator.estimateTaskExecutionTime() );
        }
        
    }

    void registerLocalTasks( TaskType l[], JobList jobs )
    {
        for( TaskType type: l ) {
            registerLocalTask( type, jobs );
        }
    }

    synchronized void printStatistics( PrintStream s, long workInterval )
    {
        for( TaskInfo info: taskInfoList ) {
            if( info != null ) {
                info.reportStats( s, workInterval );
            }
        }        
    }

    synchronized void setQueueTimePerTask( TaskType type, long queueTime, int queueLength )
    {
        TaskInfo info = getTaskInfo( type );
        info.setQueueTimePerTask( queueTime/(queueLength+1) );
    }

    synchronized void countTask( TaskType taskType, long computeInterval )
    {
        TaskInfo stats = getTaskInfo( taskType );
        stats.countTask( computeInterval );
    }

}
