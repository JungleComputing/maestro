package ibis.maestro;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * A class representing the master work queue.
 *
 * This requires a special implementation because we want to enforce
 * priorities for the different job types, and we want to know
 * which job types are currently present in the queue.
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
     * The information for one type of job in the queue.
     * 
     * @author Kees van Reeuwijk
     *
     */
    private static final class QueueType {
	/** The type of jobs in this queue. */
	final JobType type;

	/** The work queue for these jobs. */
	final LinkedList<RunJobMessage> queue = new LinkedList<RunJobMessage>();

	QueueType( JobType type ){
	    this.type = type;
	}
    }

    /**
     * Submit a new job, belonging to the task with the given identifier,
     * to the queue.
     * @param msg The job to submit.
     */
    void add( RunJobMessage msg )
    {
	JobType t = msg.job.type;

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
	    int cmp = t.jobNo-q.type.jobNo;
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
     * Given a list of workers and a submission structure to fill,
     * try to select a job and a worker to execute the job.
     * If there are no jobs in the queue, return false.
     * If there are jobs in the queue, but no workers to execute the
     * jobs, set the worker field of the submission to <code>null</code>.
     *
     * FIXME: see if we can factor out the empty queue test.
     * 
     * @param sub The submission structure to fill.
     * @param workers The list of workers to choose from.
     * @return True iff there currently is no work.
     */
    RunJobMessage remove()
    {
	// Search from highest to lowest priority for a job to execute.
	for( QueueType t: queueTypes ) {
	    if( Settings.traceWorkerProgress ){
		System.out.println( "Worker: trying to select job from " + t.type + " queue" );
	    }
		LinkedList<RunJobMessage> queue = t.queue;
	    if( !queue.isEmpty() ) {
		RunJobMessage e = queue.removeFirst();
		size--;
		if( Settings.traceWorkerProgress ){
		    System.out.println( "Worker: found a job of type " + t.type );
		}
		return e;
	    }
	}
	return null;
    }
}
