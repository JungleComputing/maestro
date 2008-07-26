package ibis.maestro;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
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
	 * @return The estimated time in ns it will take to drain all
	 *          current tasks from the queue.
	 */
	private long estimateQueueTime()
	{
	    long timePerEntry = dequeueInterval.getAverage();
	    long res = timePerEntry*elements;
	    return res;
	}

	private CompletionInfo getCompletionInfo( JobList jobs, NodeList workers )
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
		long queueTime = estimateQueueTime();
		duration = queueTime + averageCompletionTime;
	    }
	    return new CompletionInfo( previousType, duration );
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
     * Submit a new task, belonging to the job with the given identifier,
     * to the queue.
     * @param task The task to submit.
     */
    @SuppressWarnings("synthetic-access")
    void add( TaskInstance task )
    {
	TaskType type = task.type;
	TypeInfo info = getTypeInfo( type );
	int length = info.registerAdd();
        int pos = findInsertionPoint( queue, task );
	queue.add( pos, task );
        if( Settings.traceQueuing ) {
            Globals.log.reportProgress( "Adding " + task.formatJobAndType() + " at position " + pos + " of master queue; length is now " + queue.size() + "; " + length + " of type " + type );
        }
    }
    
    protected TaskInstance remove()
    {
        return queue.remove( 0 );
    }

    /**
     * Given a list of workers and a submission structure to fill,
     * try to select a task and a worker to execute the task.
     * If there are no tasks in the queue, return false.
     * If there are tasks in the queue, but no workers to execute the
     * tasks, set the worker field of the submission to <code>null</code>.
     *
     * @param sub The submission structure to fill.
     * @param workers The list of workers to choose from.
     */
    @SuppressWarnings("synthetic-access")
    int selectSubmisson( int reserved, Submission sub, NodeList workers, HashSet<TaskType> noReadyWorkers )
    {
	int ix = reserved;

	sub.worker = null;
	sub.task = null;
	while( ix<queue.size() ) {
	    TaskInstance task = queue.get( ix );
	    TaskType type = task.type;
	    if( noReadyWorkers.contains( type ) ) {
	        // There are no ready workers for this type, don't bother trying.
	        if( Settings.traceMasterQueue ){
	            System.out.println( "Skipping task of type " + type + ": no ready workers" );
	        }
	        ix++;
	        continue;
	    }
	    TypeInfo info = getTypeInfo( type );
	    WorkerTaskInfo worker = workers.selectBestWorker( type );
	    if( worker != null ) {
                if( worker.reserveIfNeeded() ) {
                    // Reserved for this worker. Don't submit it now.
                    if( Settings.traceMasterQueue ){
                        System.out.println( "Reserved a task of type " + type + " for worker " + worker );
                    }
                    ix++;
                    continue;
                }
                queue.remove( ix );
                int length = info.registerRemove();
                if( Settings.traceQueuing ) {
                    Globals.log.reportProgress( "Removing " + task.formatJobAndType() + " from master queue; length is now " + queue.size() + "; " + length + " of type " + type );
                }
                sub.task = task;
                sub.worker = worker;
                sub.predictedDuration = worker.estimateRoundtripTime();
                if( Settings.traceMasterQueue ){
                    System.out.println( "Found a worker for task type " + type );
                }
                break;
	    }
	    noReadyWorkers.add( type );
	    if( Settings.traceMasterQueue ){
		System.out.println( "No ready worker for task type " + type );
	    }
	    ix++;
	}
        return ix;
    }

    @SuppressWarnings("synthetic-access")
    void printStatistics( PrintStream s )
    {
	for( TypeInfo t: queueTypes ) {
	    if( t != null ) {
		t.printStatistics( s );
	    }
	}
    }

    @SuppressWarnings("synthetic-access")
    CompletionInfo[] getCompletionInfo( JobList jobs, NodeList workers )
    {
	CompletionInfo res[] = new CompletionInfo[queueTypes.size()];

	for( int i=0; i<res.length; i++ ) {
	    TypeInfo q = queueTypes.get( i );
            if( q != null ){
                res[i] = q.getCompletionInfo( jobs, workers );
            }
	}
	return res;
    }

    /**
     * Returns true iff the entire queue is empty.
     * @return
     */
    protected boolean isEmpty()
    {
        return queue.isEmpty();
    }

    /**
     * Adds all the task instances in the given list to the queue.
     * The list may be <code>null</code>.
     * @param l The list of task instances to add.
     */
    protected void add( List<TaskInstance> l )
    {
	if( l != null ) {
	    for( TaskInstance ti: l ) {
		add( ti );
	    }
	}
    }
}
