package ibis.maestro;

import java.io.PrintStream;

/**
 * Statistics per type for the different task types in the queue.
 * 
 * @author Kees van Reeuwijk
 */
final class WorkerQueueTypeInfo
{
    /** The type these statistics are about. */
    final TaskType type;

    /** The total number of tasks of this type that entered the queue. */
    private long taskCount = 0;

    /** Current number of elements of this type in the queue. */
    private int elements = 0;

    /** Maximal ever number of elements in the queue. */
    private int maxElements = 0;

    /** The last moment in ns that the front of the queue changed. */
    private long frontChangedTime = 0;

    /** The estimated time interval between tasks being dequeued. */
    final TimeEstimate dequeueInterval = new TimeEstimate( 1*Service.MILLISECOND_IN_NANOSECONDS );

    WorkerQueueTypeInfo( TaskType type  )
    {
        this.type = type;
    }

    void printStatistics( PrintStream s )
    {
        s.println( "worker queue for " + type + ": " + taskCount + " tasks; dequeue interval: " + dequeueInterval + "; maximal queue size: " + maxElements );
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
        taskCount++;
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

    synchronized WorkerQueueInfo getWorkerQueueInfo( long dwellTime )
    {
        return new WorkerQueueInfo( type, elements, dequeueInterval.getAverage(), dwellTime );
    }
}