package ibis.maestro;

import java.io.PrintStream;

/**
 * Information about all tasks.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInfoList
{
    private final TaskInfo taskInfoList[];

    TaskInfoList( TaskType[] taskTypes, int n )
    {
        taskInfoList = new TaskInfo[n];
        for( TaskType type: taskTypes ) {
            taskInfoList[type.index] = new TaskInfo( type );
        }
    }
    
    TaskInfo getTaskInfo( TaskType type )
    {
        return taskInfoList[type.index];
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
