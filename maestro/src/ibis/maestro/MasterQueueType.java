package ibis.maestro;

import java.io.PrintStream;
import java.util.LinkedList;

/**
 * The information for one type of task in the queue.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class MasterQueueType {
    /** The type of tasks in this queue. */
    final TaskType type;

    /** The work queue for these tasks. */
    private final LinkedList<TaskInstance> queue = new LinkedList<TaskInstance>();

    /** The number of tasks entered in this queue. */
    private int taskCount = 0;

    /** The estimated time interval between tasks being dequeued. */
    final TimeEstimate dequeueInterval = new TimeEstimate( 1*Service.MILLISECOND_IN_NANOSECONDS );

    private long frontChangedTime = 0;

    private int maxsz = 0;

    MasterQueueType( TaskType type ){
        this.type = type;
    }

    /**
     * Returns a string representation of this queue.. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "Q for " + type + "; " + queue.size() + " elm.; dQi=" + dequeueInterval; 
    }

    /**
     * Returns true iff the queue associated with this type is empty.
     * @return True iff the queue is empty.
     */
    boolean isEmpty() {
        return queue.isEmpty();
    }
    
    /** Returns the size of the queue.
     * 
     * @return The queue size.
     */
    int size() {
        return queue.size();
    }

    void add( TaskInstance j )
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

    TaskInstance removeFirst()
    {
        long now = System.nanoTime();
        if( frontChangedTime != 0 ) {
            // We know when this entry became the front of the queue.
            long i = now - frontChangedTime;
            dequeueInterval.addSample( i );
        }
        TaskInstance res = queue.removeFirst();
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

    void printStatistics( PrintStream s )
    {
        s.println( "master queue for " + type + ": " + taskCount + " tasks; dequeue interval: " + dequeueInterval + "; maximal queue size: " + maxsz );
    }

    CompletionInfo getCompletionInfo( JobList jobs, WorkerList workers )
    {
        long averageCompletionTime = workers.getAverageCompletionTime( type );
        long duration;

        if( averageCompletionTime == Long.MAX_VALUE ) {
            duration = Long.MAX_VALUE;
        }
        else {
            long queueTime = estimateQueueTime();
            duration = queueTime + averageCompletionTime;
        }
        TaskType previousType = jobs.getPreviousTaskType( type );
        if( previousType == null ) {
            return null;
        }
        return new CompletionInfo( previousType, queue.size(), duration );
    }
}
