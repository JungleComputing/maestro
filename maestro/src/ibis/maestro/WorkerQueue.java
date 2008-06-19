package ibis.maestro;

import java.util.ArrayList;
import java.util.LinkedList;

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
    private final ArrayList<QueueType> queueTypes = new ArrayList<QueueType>();
    int size = 0;

    WorkerQueue()
    {
	// Empty
    }

    /**
     * The information for one type of task in the queue.
     * 
     * @author Kees van Reeuwijk
     *
     */
    private static final class QueueType {
	/** The type of tasks in this queue. */
	final TaskType type;

	/** The work queue for these tasks. */
	final LinkedList<RunTaskMessage> queue = new LinkedList<RunTaskMessage>();

	QueueType( TaskType type ){
	    this.type = type;
	}
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
	    QueueType x = queueTypes.get( ix );
	    if( x.type.equals( t ) ) {
		x.queue.add( msg );
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
	    QueueType q = queueTypes.get( ix );
	    int cmp = t.taskNo-q.type.taskNo;
	    if( cmp>0 ){
		break;
	    }
	    ix++;
	}
	QueueType qt = new QueueType( t );
	qt.queue.add( msg );
	queueTypes.add( ix, qt );
    }

    /**
     * Returns true iff the entire queue is empty.
     * @return
     */
    boolean isEmpty()
    {
	for( QueueType t: queueTypes ) {
	    if( !t.queue.isEmpty() ) {
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
	for( QueueType t: queueTypes ) {
	    if( Settings.traceWorkerProgress ){
		System.out.println( "Worker: trying to select task from " + t.type + " queue" );
	    }
		LinkedList<RunTaskMessage> queue = t.queue;
	    if( !queue.isEmpty() ) {
		RunTaskMessage e = queue.removeFirst();
		size--;
		if( Settings.traceWorkerProgress ){
		    System.out.println( "Worker: found a task of type " + t.type );
		}
		return e;
	    }
	}
	return null;
    }
}
