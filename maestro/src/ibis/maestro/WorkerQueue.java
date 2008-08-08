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
    private final ArrayList<RunTaskMessage> queue = new ArrayList<RunTaskMessage>();
    private final WorkerQueueTaskInfo queueTypes[];
    private long queueEmptyMoment = System.nanoTime();
    private long idleDuration = 0;
    private long activeTime = 0L;

    /**
     * Given a list of supported types, constructs a new WorkerQueue.
     * @param taskTypes The list of types we support.
     * @param jobs 
     */
    WorkerQueue( TaskType[] taskTypes, JobList jobs )
    {
	queueTypes = new WorkerQueueTaskInfo[Globals.numberOfTaskTypes];
        for( TaskType t: taskTypes ) {
            WorkerQueueTaskInfo queueTypeInfo = new WorkerQueueTaskInfo( t );
	    queueTypes[t.index] = queueTypeInfo;
            Task task = jobs.getTask( t );
            if( task instanceof TaskExecutionTimeEstimator ) {
                TaskExecutionTimeEstimator estimator = (TaskExecutionTimeEstimator) task;
                queueTypeInfo.setInitialComputeTimeEstimate( estimator.estimateTaskExecutionTime() );
            }
            
        }
    }

    /**
     * Returns true iff the entire queue is empty.
     * @return
     */
    synchronized boolean isEmpty()
    {
        return queue.isEmpty();
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
    protected WorkerQueueInfo[] getWorkerQueueInfo()
    {
        WorkerQueueInfo res[] = new WorkerQueueInfo[queueTypes.length];

        for( int i=0; i<res.length; i++ ) {
            WorkerQueueTaskInfo q = queueTypes[i];

            if( q != null ){
        	res[i] = q.getWorkerQueueInfo();
            }
        }
        return res;
    }
    
    private void dumpQueue()
    {
        System.out.print( "Worker queue: " );
        for( RunTaskMessage m: queue ) {
            System.out.print( m.label() );
            System.out.print( ' ' );
        }
        System.out.println();
    }

    /** 
     * Add the given task to our queue.
     * @param msg The task to add to the queue
     */
    synchronized void add( RunTaskMessage msg )
    {
        if( activeTime == 0L ) {
            activeTime = msg.arrivalMoment;
        }
        if( queueEmptyMoment>0L ){
            // The queue was empty before we entered this
            // task in it. Record this for the statistics.
            long queueEmptyInterval = msg.arrivalMoment - queueEmptyMoment;
            idleDuration += queueEmptyInterval;
            queueEmptyMoment = 0L;
        }
        TaskType type = msg.taskInstance.type;
        WorkerQueueTaskInfo info = queueTypes[type.index];
        int length = info.registerAdd();
        int pos = findInsertionPoint( queue, msg );
        queue.add( pos, msg );
        if( Settings.traceQueuing ) {
            Globals.log.reportProgress( "Adding " + msg.taskInstance.formatJobAndType() + " at position " + pos + " of worker queue; length is now " + queue.size() + "; " + length + " of type " + type );
        }
        msg.setQueueMoment( msg.arrivalMoment, length );
        if( Settings.dumpWorkerQueue ) {
            dumpQueue();
        }
    }

    void countTask( TaskType type, long computeInterval )
    {
        WorkerQueueTaskInfo info = queueTypes[type.index];
        info.countTask( computeInterval );
    }

    void setQueueTimePerTask( TaskType type, long queueTime, int queueLength )
    {
        WorkerQueueTaskInfo info = queueTypes[type.index];
        info.setQueueTimePerTask( queueTime/(queueLength+1) );
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
        WorkerQueueTaskInfo info = queueTypes[res.taskInstance.type.index];
        int length = info.registerRemove();
        if( Settings.traceQueuing ) {
            Globals.log.reportProgress( "Removing " + res.taskInstance.formatJobAndType() + " from worker queue; length is now " + queue.size() + "; " + length + " of type " + res.taskInstance.type );
        }
        return res;
    }

    synchronized long getActiveTime( long startTime )
    {
        if( activeTime<startTime ) {
            System.err.println( "Worker was not used" );
            return startTime;
        }
        return activeTime;
    }


    synchronized void printStatistics( PrintStream s, long workInterval )
    {
        double idlePercentage = 100.0*((double) idleDuration/(double) workInterval);
        s.println( "Worker: total idle time = " + Service.formatNanoseconds( idleDuration ) + String.format( " (%.1f%%)", idlePercentage ) );
        for( WorkerQueueTaskInfo t: queueTypes ) {
            if( t != null ) {
                t.printStatistics( s, workInterval );
            }
        }
    }

    WorkerQueueTaskInfo getTaskInfo( TaskType type )
    {
        return queueTypes[type.index];
    }

    void registerNode( NodeInfo nodeInfo )
    {
        for( WorkerQueueTaskInfo info: queueTypes )
        {
            if( info != null ) {
                info.registerNode( nodeInfo );
            }
        }
    }
}
