package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
final class MasterQueue
{
    int taskCount = 0;
    private final ArrayList<TypeInfo> queueTypes = new ArrayList<TypeInfo>();
    protected final ArrayList<TaskInstance> queue = new ArrayList<TaskInstance>();

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

        private void printStatistics( PrintStream s )
        {
            s.println( "master queue for " + type + ": " + taskCount + " tasks; dequeue interval: " + dequeueInterval + "; maximal queue size: " + maxElements );
        }

        private int registerAdd()
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

        /**
         * @param idleProcessors The number of processors that are currently idle.
         * @return The estimated time in ns it will take to drain all
         *          current tasks from the queue.
         */
        private long estimateQueueTime( int idleProcessors )
        {
            long timePerEntry = dequeueInterval.getAverage();
            // Since at least one processor isn't working on a task (or we
            // wouldn't be here), we are only impressed if there is more
            // than one idle processor.
            int extra = idleProcessors>1?0:1;
            long res = timePerEntry*(extra+elements);
            return res;
        }

        private CompletionInfo getCompletionInfo( JobList jobs, NodeList workers, int idleProcessors )
        {
            TaskType previousType = jobs.getPreviousTaskType( type );
            if( previousType == null ) {
                return null;
            }
            long averageCompletionTime = workers.getAverageCompletionTime( type );
            long duration;

            if( averageCompletionTime == Long.MAX_VALUE ) {
                duration = Long.MAX_VALUE;
            }
            else {
                long queueTime = estimateQueueTime( idleProcessors );
                duration = queueTime + averageCompletionTime;
            }
            return new CompletionInfo( previousType, duration );
        }
    }

    /**
     * Constructs a new MasterQueue.
     * @param taskTypes The supported types.
     */
    MasterQueue( TaskType[] taskTypes )
    {
        // FIXME: create a plain array.
        for( TaskType t: taskTypes ) {
            getTypeInfo( t );  // Make sure the type administration is there.
        }
        // TODO: after this point no new types should have to be added.
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

    private static int findInsertionPoint( ArrayList<TaskInstance> queue, TaskInstance e )
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
        long id = e.jobInstance.id;
        while( true ){
            int mid = (start+end)/2;
            if( mid == start ){
                break;
            }
            long midId = queue.get( mid ).jobInstance.id;
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
        long startId = queue.get( start ).jobInstance.id;
        if( startId<id ){
            return end;
        }
        return start;
    }

    /**
     * Returns true iff the entire queue is empty.
     * @return
     */
    synchronized boolean isEmpty()
    {
        return queue.isEmpty();
    }

    @SuppressWarnings("synthetic-access")
    private void unsynchronizedAdd( TaskInstance task )
    {
        taskCount++;
        TaskType type = task.type;
        TypeInfo info = getTypeInfo( type );
        int length = info.registerAdd();
        int pos = findInsertionPoint( queue, task );
        queue.add( pos, task );
        if( Settings.traceQueuing ) {
            Globals.log.reportProgress( "Adding " + task.formatJobAndType() + " at position " + pos + " of master queue; length is now " + queue.size() + "; " + length + " of type " + type );
        }
    }

    /**
     * Submit a new task, belonging to the job with the given identifier,
     * to the queue.
     * @param task The task to submit.
     */
    protected synchronized void add( TaskInstance task )
    {
        unsynchronizedAdd( task );
    }

    /**
     * Adds all the task instances in the given list to the queue.
     * The list may be <code>null</code>.
     * @param l The list of task instances to add.
     */
    protected synchronized void add( List<TaskInstance> l )
    {
        if( l != null ) {
            for( TaskInstance ti: l ) {
                unsynchronizedAdd( ti );
            }
        }
    }

    protected TaskInstance remove()
    {
        return queue.remove( 0 );
    }

    @SuppressWarnings("synthetic-access")
    void printStatistics( PrintStream s )
    {
        for( TypeInfo t: queueTypes ) {
            if( t != null ) {
                t.printStatistics( s );
            }
        }
        s.printf(  "Master: # incoming tasks = %5d\n", taskCount );
    }

    @SuppressWarnings("synthetic-access")
    synchronized CompletionInfo[] getCompletionInfo( JobList jobs, NodeList workers, int idleProcessors )
    {
        CompletionInfo res[] = new CompletionInfo[queueTypes.size()];

        for( int i=0; i<res.length; i++ ) {
            TypeInfo q = queueTypes.get( i );
            if( q != null ){
                res[i] = q.getCompletionInfo( jobs, workers, idleProcessors );
            }
        }
        return res;
    }

    /**
     * Given a task type, select the best worker from the list that has a
     * free slot. In this context 'best' is simply the worker with the
     * shortest overall completion time.
     *  
     * @param type The type of task we want to execute.
     * @return The info of the best worker for this task, or <code>null</code>
     *         if there currently aren't any workers for this task type.
     */
    private boolean selectBestWorker( Submission sub, HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap, NodeUpdateInfo tables[], TaskType type )
    {
        NodeUpdateInfo best = null;
        long bestInterval = Long.MAX_VALUE;
        long predictedDuration = 0l;

        for( NodeUpdateInfo info: tables ) {
            LocalNodeInfo localNodeInfo = localNodeInfoMap.get( info.source );
            long val = info.estimateJobCompletion( localNodeInfo, type );

            if( val<Long.MAX_VALUE ) {
                if( val<bestInterval ) {
                    bestInterval = val;
                    best = info;
                    predictedDuration = localNodeInfo.getPredictedDuration( type );
                }
            }
        }

        if( best == null ) {
            if( Settings.traceMasterQueue ){
                Globals.log.reportProgress( "No workers for task of type " + type );
            }
            return false;
        }
        if( Settings.traceMasterQueue ){
            Globals.log.reportProgress( "Selected worker " + best.source + " for task of type " + type );
        }
        sub.worker = best.source;
        sub.predictedDuration = predictedDuration;
        return true;
    }

    /**
     * @param tables  
     * @return
     */
    synchronized Submission getSubmission( HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap, NodeUpdateInfo[] tables )
    {
        int ix = 0;
        Submission sub = new Submission();
        while( ix<queue.size() ) {
            TaskInstance task = queue.get( ix );
            TaskType type = task.type;
            boolean ok = selectBestWorker( sub, localNodeInfoMap, tables, type );
            if( ok ) {
                queue.remove( ix );
                TypeInfo info = getTypeInfo( type );
                int length = info.registerRemove();
                if( Settings.traceMasterQueue || Settings.traceQueuing ) {
                    Globals.log.reportProgress( "Removing " + task.formatJobAndType() + " from master queue; length is now " + queue.size() + "; " + length + " of type " + type );
                }
                sub.task = task;
                return sub;
            }
            if( Settings.traceMasterQueue ){
                System.out.println( "No ready worker for task type " + type );
            }
            ix++;
        }
        return null;
    }

    synchronized boolean hasWork()
    {
        return !queue.isEmpty();
    }
}
