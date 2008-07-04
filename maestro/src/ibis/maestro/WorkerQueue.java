package ibis.maestro;

import java.util.ArrayList;
import java.util.HashMap;

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
    private final ArrayList<WorkerQueueType> queueTypes = new ArrayList<WorkerQueueType>();
    int size = 0;

    WorkerQueue()
    {
	// Empty
    }

    protected WorkerQueueInfo[] getWorkerQueueInfo(HashMap<TaskType, WorkerTaskStats> taskStats)
    {
	WorkerQueueInfo res[] = new WorkerQueueInfo[queueTypes.size()];

	for( int i=0; i<res.length; i++ ) {
	    WorkerQueueType workerQueueType = queueTypes.get( i );
	    WorkerTaskStats stats = taskStats.get( workerQueueType.type );
	    res[i] = workerQueueType.getWorkerQueueInfo( stats.getEstimatedDwellTime() );
	}
	return res;
    }

    /**
     * Submit a new task, belonging to the job with the given identifier,
     * to the queue.
     * @param msg The task to submit.
     */
    void add( RunTaskMessage msg )
    {
	TaskType t = msg.task.type;

	size++;
	// TODO: since we have an ordered list, use binary search.
	int ix = queueTypes.size();
	while( ix>0 ) {
	    ix--;
	    WorkerQueueType x = queueTypes.get( ix );
	    if( x.type.equals( t ) ) {
		x.add( msg );
		return;
	    }
	}
	if( Settings.traceWorkerProgress ){
	    System.out.println( "Worker: registering queue for new type " + t );
	}
	// This is a new type. Insert it in the right place
	// to keep the queues ordered from highest to lowest
	// priority.
	ix = 0;
	while( ix<queueTypes.size() ){
	    WorkerQueueType q = queueTypes.get( ix );
	    int cmp = t.taskNo-q.type.taskNo;
	    if( cmp>0 ){
		break;
	    }
	    ix++;
	}
	WorkerQueueType qt = new WorkerQueueType( t );
	qt.add( msg );
	queueTypes.add( ix, qt );
    }

    /**
     * Returns true iff the entire queue is empty.
     * @return
     */
    boolean isEmpty()
    {
	for( WorkerQueueType t: queueTypes ) {
	    if( !t.isEmpty() ) {
		return false;
	    }
	}
	return true;
    }

    /**
     * Given a list of workers and a subjob structure to fill,
     * try to select a task and a worker to execute the task.
     * If there are no tasks in the queue, return false.
     * If there are tasks in the queue, but no workers to execute the
     * tasks, set the worker field of the subjob to <code>null</code>.
     *
     * FIXME: see if we can factor out the empty queue test.
     * 
     * @param sub The subjob structure to fill.
     * @param workers The list of workers to choose from.
     * @return True iff there currently is no work.
     */
    RunTaskMessage remove()
    {
	// Search from highest to lowest priority for a task to execute.
	for( WorkerQueueType queue: queueTypes ) {
	    if( Settings.traceWorkerProgress ){
		System.out.println( "Worker: trying to select task from " + queue.type + " queue" );
	    }
	    if( !queue.isEmpty() ) {
		RunTaskMessage e = queue.removeFirst();
		size--;
		if( Settings.traceWorkerProgress ){
		    System.out.println( "Worker: found a task of type " + queue.type );
		}
		return e;
	    }
	}
	return null;
    }
}
