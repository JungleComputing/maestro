package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;

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
    private final ArrayList<MasterQueueType> queueTypes = new ArrayList<MasterQueueType>();
    int size = 0;

    MasterQueue()
    {
        // Empty
    }

    /**
     * Submit a new job, belonging to the task with the given identifier,
     * to the queue.
     * @param j The job to submit.
     * @return The estimated time in ns this job will linger in the queue.
     */
    long submit( JobInstance j )
    {
        JobType t = j.type;

        size++;
        // TODO: since we have an ordered list, use binary search.
        int ix = queueTypes.size();
        while( ix>0 ) {
            ix--;
            MasterQueueType x = queueTypes.get( ix );
            if( x.type.equals( t ) ) {
                x.add( j );
                return x.estimateQueueTime();
            }
        }
        if( Settings.traceMasterQueue ){
            System.out.println( "Master queue: registering new type " + t );
        }
        // This is a new type. Insert it in the right place
        // to keep the queues ordered from highest to lowest
        // priority.
        ix = 0;
        while( ix<queueTypes.size() ){
            MasterQueueType q = queueTypes.get( ix );
            int cmp = t.jobNo-q.type.jobNo;
            if( cmp>0 ){
                break;
            }
            ix++;
        }
        MasterQueueType qt = new MasterQueueType( t );
        qt.add( j );
        queueTypes.add( ix, qt );
        return 0l;   // We haven't had time to build a time estimate anyway, so don't bother to call estimateQueueTime().
    }

    /**
     * Returns true iff the entire queue is empty.
     * @return
     */
    boolean isEmpty()
    {
        for( MasterQueueType t: queueTypes ) {
            if( !t.isEmpty() ) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given a worker, try to increment the allowance of one of its
     * supported types. We only increment the allowance of a job
     * type that is currently present in the queue.
     * @param workerID The id of the worker to increase the allowance for.
     * @param workers The list of workers of the master.
     */
    void incrementAllowance( WorkerIdentifier workerID, WorkerList workers )
    {
        for( MasterQueueType t: queueTypes ) {
            if( !t.isEmpty() ) {
                // There are jobs of this type in the queue.
                if( workers.incrementAllowance( workerID, t.type ) ) {
                    // We could increment the allowance. Mission accomplished.
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
    boolean selectSubmisson( Submission sub, WorkerList workers )
    {
        boolean noWork = true;
        sub.worker = null;
        sub.job = null;
        for( MasterQueueType t: queueTypes ) {
            if( Settings.traceMasterQueue ){
                System.out.println( "Trying to select job from " + t.type + " queue" );
            }
            if( t.isEmpty() ) {
                if( Settings.traceMasterQueue ){
                    System.out.println( t.type + " queue is empty" );
                }
            }
            else {
                WorkerInfo worker = workers.selectBestWorker( t.type );

                noWork = false; // There is at least one queue with work.
                if( worker == null ) {
                    if( Settings.traceMasterQueue ){
                        System.out.println( "No ready worker for job type " + t.type );
                    }
                }
                else {
                    JobInstance e = t.removeFirst();
                    sub.job = e;
                    sub.worker = worker;
                    size--;
                    if( Settings.traceMasterQueue ){
                        System.out.println( "Found a worker for job type " + t.type );
                    }
                    break;
                }
            }
        }
        return noWork;
    }

    /**
     * Returns the number of elements in this work queue.
     * @return The number of elements in this queue.
     */
    int size()
    {
        return size;
    }
    
    void printStatistics( PrintStream s )
    {
        for( MasterQueueType t: queueTypes ) {
            t.printStatistics( s );
        }
    }

    CompletionInfo[] getCompletionInfo(WorkerList workers)
    {
	CompletionInfo res[] = new CompletionInfo[queueTypes.size()];
	
	for( int i=0; i<res.length; i++ ) {
	    MasterQueueType q = queueTypes.get(i);
	    res[i] = q.getCompletionInfo( workers );
	}
	return res;
    }
}
