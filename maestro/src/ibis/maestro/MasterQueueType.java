package ibis.maestro;

import java.io.PrintStream;
import java.util.LinkedList;

/**
 * The information for one type of job in the queue.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class MasterQueueType {
    /** The type of jobs in this queue. */
    final JobType type;

    /** The work queue for these jobs. */
    private final LinkedList<JobInstance> queue = new LinkedList<JobInstance>();

    /** The number of jobs entered in this queue. */
    private int jobCount = 0;

    /** The estimated time interval between jobs being dequeued. */
    final TimeEstimate dequeueInterval = new TimeEstimate( 1*Service.MILLISECOND_IN_NANOSECONDS );

    private long frontChangedTime = 0;

    private int maxsz = 0;

    MasterQueueType( JobType type ){
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

    void add( JobInstance j )
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
        jobCount++;
    }

    JobInstance removeFirst()
    {
        long now = System.nanoTime();
        if( frontChangedTime != 0 ) {
            // We know when this entry became the front of the queue.
            long i = now - frontChangedTime;
            dequeueInterval.addSample( i );
        }
        JobInstance res = queue.removeFirst();
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
     *          current jobs from the queue.
     */
    long estimateQueueTime()
    {
        long timePerEntry = dequeueInterval.getAverage();
        long res = timePerEntry*queue.size();
        return res;
    }

    void printStatistics( PrintStream s )
    {
        s.println( "master queue for " + type + ": " + jobCount + " jobs; dequeue interval: " + dequeueInterval + "; maximal queue size: " + maxsz );
    }

    CompletionInfo getCompletionInfo( TaskList tasks, WorkerList workers )
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
        JobType previousType = tasks.getPreviousJobType( type );
        if( previousType == null ) {
            return null;
        }
        return new CompletionInfo( previousType, duration );
    }
}