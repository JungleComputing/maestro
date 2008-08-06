package ibis.maestro;

import java.io.PrintStream;
import java.util.LinkedList;

/**
 * Information on a task on the master.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInfo
{
    private LinkedList<NodeTaskInfo> workers = new LinkedList<NodeTaskInfo>();
    final TaskType type;
    private int taskCount = 0;
    private long totalWorkTime = 0;        
    private long totalQueueTime = 0;     // Cumulative queue time of all tasks.
    final TimeEstimate averageComputeTime = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );
    final TimeEstimate queueTimePerTask = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );

    TaskInfo( TaskType type )
    {
        this.type = type;
    }

    /**
     * Returns a string representation of this task info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return String.format( "type=" + type + " workers: ", workers.size() );
    }


    /**
     * Returns the best average completion time for this task.
     * We compute this by taking the minimum over all our workers.
     * @return The best average completion time of our workers.
     */
    synchronized long getAverageCompletionTime()
    {
        long res = Long.MAX_VALUE;

        for( NodeTaskInfo wi: workers ) {
            long val = wi.getAverageCompletionTime();

            if( val<res ) {
                res = val;
            }
        }
        return res;
    }


    /**
     * Registers the completion of a task of this particular type, with the
     * given queue interval and the given work interval.
     * @param queueTime The time this task spent in the queue.
     * @param workTime The time it took to execute this task.
     */
    void countTask( long workTime )
    {
        taskCount++;
        totalWorkTime += workTime;
        averageComputeTime.addSample( workTime );
    }

    void reportStats( PrintStream out, double workTime )
    {
        double workPercentage = 100.0*(totalWorkTime/workTime);
        if( taskCount>0 ) {
            out.println( "Worker: " + type + ":" );
            out.printf( "    # tasks          = %5d\n", taskCount );
            out.println( "    total work time = " + Service.formatNanoseconds( totalWorkTime ) + String.format( " (%.1f%%)", workPercentage )  );
            out.println( "    queue time/task  = " + Service.formatNanoseconds( totalQueueTime/taskCount ) );
            out.println( "    work time/task   = " + Service.formatNanoseconds( totalWorkTime/taskCount ) );
            out.println( "    aver. dwell time = " + Service.formatNanoseconds( (totalWorkTime+totalQueueTime)/taskCount ) );
        }
        else {
            out.println( "Worker: " + type + " is unused" );
        }
    }

    /**
     * @param queueLength The current length of the work queue for this type.
     * @return The estimated current dwell time on this worker for this task.
     */
    long getEstimatedDwellTime( int queueLength )
    {
        long res = averageComputeTime.getAverage() + queueTimePerTask.getAverage()*queueLength;
        return res;
    }

    /**
     * Update the estimate for the queue time per task.
     * @param v The new value for the queue time per task.
     */
    void setQueueTimePerTask( long v )
    {
        totalQueueTime += v;
        queueTimePerTask.addSample( v );
    }

    /** Returns the estimated time to compute this task.
     * @return The estimated time.
     */
    synchronized long getEstimatedComputeTime()
    {
        return averageComputeTime.getAverage();
    }

    /** 
     * Sets the initial compute time estimate of this task to the given value.
     * @param estimate The initial estimate.
     */
    void setInitialComputeTimeEstimate( long estimate )
    {
        averageComputeTime.setInitialEstimate( estimate );
    }

}
