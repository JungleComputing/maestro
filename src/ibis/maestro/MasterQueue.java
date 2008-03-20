package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;

import java.util.AbstractList;
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
final class MasterQueue {
    private final TypeInformation typeInformation;
    private final AbstractList<QueueType> queueTypes = new ArrayList<QueueType>();

    MasterQueue( TypeInformation typeInformation )
    {
        this.typeInformation = typeInformation;
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
	final LinkedList<QueueEntry> queue = new LinkedList<QueueEntry>();
	
	QueueType( JobType type ){
	    this.type = type;
	}

	/**
	 * Returns true iff the queue associated with this type is empty.
	 * @return True iff the queue is empty.
	 */
	boolean isEmpty() {
	    return queue.isEmpty();
	}
    }

    private static final class QueueEntry {
        final Job job;
        final TaskIdentifier taskId;

        QueueEntry(Job job, TaskIdentifier id) {
            this.job = job;
            this.taskId = id;
        }
    }

    void submit( Job j, TaskIdentifier taskId )
    {
        QueueEntry e = new QueueEntry( j, taskId );
        JobType t = j.getType();
        
        // TODO: order the types by priority.
        // TODO: in an ordered list, use binary search.
        int ix = queueTypes.size();
        while( ix>0 ) {
            ix--;
            QueueType x = queueTypes.get( ix );
            if( x.type.equals( t ) ) {
        	x.queue.add( e );
        	return;
            }
        }
        // This is a new type.
        QueueType qt = new QueueType( t );
        qt.queue.add( e );
        queueTypes.add( qt );
    }

    /**
     * Returns true iff the entire queue is empty.
     * @return
     */
    boolean isEmpty() {
	for( QueueType t: queueTypes ) {
	    if( !t.isEmpty() ) {
		return false;
	    }
	}
	return true;
    }

    void incrementAllowance( WorkerIdentifier workerID, WorkerList workers )
    {
        // We already know that this worker can handle this type of
        // job, but he asks for a larger allowance.
        // We only increase it if at the moment there is a job of this
        // type in the queue.
	//
	// FIXME: once we have priorities, walk the list in order of
	// decreasing priority.
	for( QueueType t: queueTypes ) {
	    if( !t.isEmpty() ) {
		if( workers.incrementAllowance(workerID, t.type ) ) {
		    break;
		}
	    }
	}
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
    boolean selectJob( Submission sub, WorkerList workers )
    {
	boolean noWork = true;
        sub.worker = null;
        sub.taskId = null;
        sub.job = null;
	for( QueueType t: queueTypes ) {
	    if( !t.isEmpty() ) {
		noWork = false; // There is at least one queue with work.
		WorkerInfo worker = workers.selectBestWorker( t.type );
		if( worker != null ) {
		    QueueEntry e = t.queue.removeFirst();
		    sub.job = e.job;
		    sub.taskId = e.taskId;
                    sub.worker = worker;
                    break;
		}
	    }
	}
	return noWork;
    }
}
