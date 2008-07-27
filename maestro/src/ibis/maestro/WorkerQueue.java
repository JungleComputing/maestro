package ibis.maestro;

import java.io.PrintStream;
import java.util.ArrayList;

/**
 * A class representing the master work queue.
 *
 * This requires a special implementation because we want to enforce
 * priorities for the different task types, and we want to know
 * which task types are currently present in the queue.
 *
 * @author Kees van Reeuwijk
 *
 */
final class WorkerQueue {
    protected final ArrayList<RunTaskMessage> queue = new ArrayList<RunTaskMessage>();
    private final ArrayList<TypeInfo> queueTypes = new ArrayList<TypeInfo>();
    long queueEmptyMoment = System.nanoTime();
    private long idleDuration = 0;

    /**
     * Returns true iff the entire queue is empty.
     * @return
     */
    protected boolean isEmpty()
    {
        return queue.isEmpty();
    }

    /**
     * Statistics per type for the different task types in the queue.
     * 
     * @author Kees van Reeuwijk
     */
    private static final class TypeInfo {
        /** The type these statistics are about. */
        final TaskType type;

        /** The total number of tasks of this type that entered the queue. */
        private long taskCount = 0;

        /** Current number of elements of this type in the queue. */
        private int elements = 0;

        /** Maximal ever number of elements in the queue. */
        private int maxElements = 0;

        private long frontChangedTime = 0;

        /** The estimated time interval between tasks being dequeued. */
        final TimeEstimate dequeueInterval = new TimeEstimate( 1*Service.MILLISECOND_IN_NANOSECONDS );

        TypeInfo( TaskType type  )
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

        WorkerQueueInfo getWorkerQueueInfo( long dwellTime )
        {
            return new WorkerQueueInfo( type, elements, dequeueInterval.getAverage(), dwellTime );
        }
    }

    private TypeInfo getTypeInfo( TaskType t )
    {
        int ix = t.index;
        while( queueTypes.size()<ix+1 ) {
            queueTypes.add( null );
        }
        TypeInfo res = queueTypes.get( ix );
        if( res == null ) {
            res = new TypeInfo( t );
            queueTypes.set( ix, res );
        }
        return res;
    }

    private static int findInsertionPoint( ArrayList<RunTaskMessage> queue, RunTaskMessage msg )
    {
        // Good old binary search.
        int start = 0;
        int end = queue.size();
        if( end == 0 ){
            // The queue is empty. This is the only case where start
            // points to a non-existent element, so we have to treat
            // it separately.
            return 0;
        }
        long id = msg.taskInstance.jobInstance.id;
        while( true ){
            int mid = (start+end)/2;
            if( mid == start ){
                break;
            }
            long midId = queue.get( mid ).taskInstance.jobInstance.id;
            if( midId<id ){
                // Mid should come before us.
                start = mid;
            }
            else {
                // Mid should come after us.
                end = mid;
            }
        }
        // This comparison is probably rarely necessary, but corner cases
        // are a pain, so I'm safe rather than sorry.
        long startId = queue.get( start ).taskInstance.jobInstance.id;
        if( startId<id ){
            return end;
        }
        return start;
    }

    @SuppressWarnings("synthetic-access")
    protected synchronized WorkerQueueInfo[] getWorkerQueueInfo( ArrayList<WorkerTaskStats> taskStats )
    {
        WorkerQueueInfo res[] = new WorkerQueueInfo[queueTypes.size()];

        for( int i=0; i<res.length; i++ ) {
            TypeInfo q = queueTypes.get( i );

            if( q != null ){
        	int ix = q.type.index;
                WorkerTaskStats stats;
        	if( ix<taskStats.size() ) {
        	    stats = taskStats.get( q.type.index );
        	}
        	else {
        	    stats = null;
        	}

                if( stats == null ) {
                    res[i] = null;
                }
                else {
                    long computeTime = stats.getEstimatedComputeTime();
                    res[i] = q.getWorkerQueueInfo( computeTime );
                }
            }
        }
        return res;
    }

    /**
     * Submit a new task, belonging to the job with the given identifier,
     * to the queue.
     * @param msg The task to submit.
     * @return The new queue length.
     */
    private int add( RunTaskMessage msg )
    {
        TaskType type = msg.taskInstance.type;
        TypeInfo info = getTypeInfo( type );
        int length = info.registerAdd();
        int pos = findInsertionPoint( queue, msg );
        queue.add( pos, msg );
        if( Settings.traceQueuing ) {
            Globals.log.reportProgress( "Adding " + msg.taskInstance.formatJobAndType() + " at position " + pos + " of worker queue; length is now " + queue.size() + "; " + length + " of type " + type );
        }
        return length;
    }

    synchronized void printStatistics( PrintStream s, long workInterval )
    {
        double idlePercentage = 100.0*((double) idleDuration/(double) workInterval);
        s.println( "Worker: total idle time = " + Service.formatNanoseconds( idleDuration ) + String.format( " (%.1f%%)", idlePercentage ) );
        for( TypeInfo t: queueTypes ) {
            if( t != null ) {
                t.printStatistics( s );
            }
        }
    }

    synchronized RunTaskMessage remove()
    {
        if( queue.isEmpty() ) {
            if( queueEmptyMoment == 0 ) {
                queueEmptyMoment = System.nanoTime();
            }
            return null;
        }
        RunTaskMessage res = queue.remove( 0 );
        TypeInfo info = getTypeInfo( res.taskInstance.type );
        int length = info.registerRemove();
        if( Settings.traceQueuing ) {
            Globals.log.reportProgress( "Removing " + res.taskInstance.formatJobAndType() + " from worker queue; length is now " + queue.size() + "; " + length + " of type " + res.taskInstance.type );
        }
        return res;
    }

    /** 
     * Add the given task to our queue.
     * @param msg The task to add to the queue
     * @param arrivalMoment The moment in ns it arrived/
     */
    synchronized void add( RunTaskMessage msg, long arrivalMoment )
    {
        if( queueEmptyMoment>0L ){
            // The queue was empty before we entered this
            // task in it. Record this for the statistics.
            long queueEmptyInterval = arrivalMoment - queueEmptyMoment;
            idleDuration += queueEmptyInterval;
            queueEmptyMoment = 0L;
        }
        int length = add( msg );
        msg.setQueueMoment( arrivalMoment, length );
    }
}
