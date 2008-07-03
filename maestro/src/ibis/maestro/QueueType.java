package ibis.maestro;

import java.io.PrintStream;
import java.util.LinkedList;

/**
 * A queue in the master or worker for a particular job type.
 *
 * @author Kees van Reeuwijk
 *
 */
abstract class QueueType<T> {

    /** The type of tasks in this queue. */
    protected final TaskType type;

    /** The work queue for these tasks. */
    protected final LinkedList<T> queue = new LinkedList<T>();

    /** The estimated time interval between tasks being dequeued. */
    final TimeEstimate dequeueInterval = new TimeEstimate( 1*Service.MILLISECOND_IN_NANOSECONDS );

    protected long frontChangedTime = 0;
    /** The number of tasks entered in this queue. */
    private int taskCount = 0;

    private int maxsz = 0;

    QueueType( TaskType type ) {
	this.type = type;
    }

    
    boolean isEmpty()
    {
	return queue.isEmpty();
    }
    
    int size()
    {
	return queue.size();
    }

    void printStatistics( PrintStream s, String role )
    {
        s.println( role + " queue for " + type + ": " + taskCount + " tasks; dequeue interval: " + dequeueInterval + "; maximal queue size: " + maxsz );
    }

    void add( T j )
    {
        queue.add( j );
        int sz = queue.size();
        if( sz>maxsz ) {
            maxsz = sz;
        }
        if( frontChangedTime == 0 ) {
    	// This entry is the front of the queue,
    	// record the time it became this.
    	frontChangedTime = System.nanoTime();
        }
        taskCount++;
    }

    T removeFirst()
    {
        long now = System.nanoTime();
        if( frontChangedTime != 0 ) {
            // We know when this entry became the front of the queue.
            long i = now - frontChangedTime;
            dequeueInterval.addSample( i );
        }
        T res = queue.removeFirst();
        if( queue.isEmpty() ) {
            // Don't take the next dequeuing into account,
            // since the queue is now empty.
            frontChangedTime = 0l;
        }
        else {
            frontChangedTime = now;
        }
        return res;
    }

    /**
     * @return The estimated time in ns it will take to drain all
     *          current tasks from the queue.
     */
    long estimateQueueTime()
    {
        long timePerEntry = dequeueInterval.getAverage();
        long res = timePerEntry*queue.size();
        return res;
    }

}
