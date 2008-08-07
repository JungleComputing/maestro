package ibis.maestro;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Statistics per type for the different task types in the queue.
 * 
 * @author Kees van Reeuwijk
 */
final class WorkerQueueTaskInfo
{
    /** The type these statistics are about. */
    final TaskType type;

    /** The workers that are willing to execute this task. */
    private final List<NodeTaskInfo> workers = new LinkedList<NodeTaskInfo>(); 

    /** The total number of tasks of this type that entered the queue. */
    private long incompingTaskCount = 0;

    private int outGoingTaskCount = 0;

    /** Current number of elements of this type in the queue. */
    private int elements = 0;

    /** Maximal ever number of elements in the queue. */
    private int maxElements = 0;

    /** The last moment in ns that the front of the queue changed. */
    private long frontChangedTime = 0;

    private final TimeEstimate queueTimePerTask = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );

    /** The estimated time interval between tasks being dequeued. */
    private final TimeEstimate dequeueInterval = new TimeEstimate( 1*Service.MILLISECOND_IN_NANOSECONDS );

    private long totalWorkTime = 0;        
    private long totalQueueTime = 0;     // Cumulative queue time of all tasks.
    private final TimeEstimate averageComputeTime = new TimeEstimate( Service.MILLISECOND_IN_NANOSECONDS );

    WorkerQueueTaskInfo( TaskType type  )
    {
        this.type = type;
    }

    synchronized void printStatistics( PrintStream s, long workTime )
    {
        s.println( "worker queue for " + type + ": " + incompingTaskCount + " tasks; dequeue interval: " + dequeueInterval + "; maximal queue size: " + maxElements );
        double workPercentage = 100.0*((double) totalWorkTime/workTime);
        PrintStream out = s;
        if( outGoingTaskCount>0 ) {
            out.println( "Worker: " + type + ":" );
            out.printf( "    # tasks          = %5d\n", outGoingTaskCount );
            out.println( "    total work time = " + Service.formatNanoseconds( totalWorkTime ) + String.format( " (%.1f%%)", workPercentage )  );
            out.println( "    queue time/task  = " + Service.formatNanoseconds( totalQueueTime/outGoingTaskCount ) );
            out.println( "    work time/task   = " + Service.formatNanoseconds( totalWorkTime/outGoingTaskCount ) );
            out.println( "    aver. dwell time = " + Service.formatNanoseconds( (totalWorkTime+totalQueueTime)/outGoingTaskCount ) );
        }
        else {
            out.println( "Worker: " + type + " is unused" );
        }
    }

    int registerAdd()
    {
        elements++;
        if( elements>maxElements ) {
            maxElements = elements;
        }
        if( frontChangedTime == 0 ) {
            // This entry is the front of the queue,
            // record the time it became this.
            frontChangedTime = System.nanoTime();
        }
        incompingTaskCount++;
        return elements;
    }

    int registerRemove()
    {
        long now = System.nanoTime();
        if( frontChangedTime != 0 ) {
            // We know when this entry became the front of the queue.
            long i = now - frontChangedTime;
            dequeueInterval.addSample( i );
        }
        elements--;
        if( elements == 0 ) {
            // Don't take the next dequeuing into account,
            // since the queue is now empty.
            frontChangedTime = 0l;
        }
        else {
            frontChangedTime = now;
        }
        return elements;
    }

    /**
     * Registers the completion of a task of this particular type, with the
     * given queue interval and the given work interval.
     * @param queueTime The time this task spent in the queue.
     * @param workTime The time it took to execute this task.
     */
    synchronized void countTask( long workTime )
    {
        outGoingTaskCount++;
        totalWorkTime += workTime;
        averageComputeTime.addSample( workTime );
    }

    /** 
     * Sets the initial compute time estimate of this task to the given value.
     * @param estimate The initial estimate.
     */
    void setInitialComputeTimeEstimate( long estimate )
    {
        averageComputeTime.setInitialEstimate( estimate );
    }

    synchronized WorkerQueueInfo getWorkerQueueInfo()
    {
        return new WorkerQueueInfo( type, elements, dequeueInterval.getAverage(), averageComputeTime.getAverage() );
    }

    /**
     * Update the estimate for the queue time per task.
     * @param v The new value for the queue time per task.
     */
    synchronized void setQueueTimePerTask( long v )
    {
        totalQueueTime += v;
        queueTimePerTask.addSample( v );
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

    void registerNode( NodeInfo nodeInfo )
    {
        NodeTaskInfo nodeTaskInfo = nodeInfo.get( type );
        synchronized( this ) {
            if( nodeTaskInfo != null ) {
                workers.add( nodeTaskInfo );
            }
        }
    }
}