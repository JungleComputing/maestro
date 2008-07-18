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
final class TypePriorityMasterQueue {
    private final ArrayList<MasterQueueType> queueTypes = new ArrayList<MasterQueueType>();
    int size = 0;

    TypePriorityMasterQueue()
    {
        // Empty
    }

    /**
     * Submit a new task, belonging to the job with the given identifier,
     * to the queue.
     * @param j The task to submit.
     * @return The estimated time in ns this task will linger in the queue.
     */
    long submit( TaskInstance j )
    {
        TaskType t = j.type;

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
            int cmp = t.taskNo-q.type.taskNo;
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
    boolean selectSubmisson( Subtask sub, WorkerList workers )
    {
        boolean noWork = true;
        sub.worker = null;
        sub.task = null;
        for( MasterQueueType t: queueTypes ) {
            if( Settings.traceMasterQueue ){
                System.out.println( "Trying to select task from " + t.type + " queue" );
            }
            if( t.isEmpty() ) {
                if( Settings.traceMasterQueue ){
                    System.out.println( t.type + " queue is empty" );
                }
            }
            else {
                WorkerTaskInfo worker = workers.selectBestWorker( t.type );

                noWork = false; // There is at least one queue with work.
                if( worker == null ) {
                    if( Settings.traceMasterQueue ){
                        System.out.println( "No ready worker for task type " + t.type );
                    }
                }
                else {
                    TaskInstance e = t.removeFirst();
                    sub.task = e;
                    sub.worker = worker;
                    size--;
                    if( Settings.traceMasterQueue ){
                        System.out.println( "Found a worker for task type " + t.type );
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

    CompletionInfo[] getCompletionInfo( JobList jobs, WorkerList workers )
    {
	CompletionInfo res[] = new CompletionInfo[queueTypes.size()];
	
	for( int i=0; i<res.length; i++ ) {
	    MasterQueueType q = queueTypes.get( i );
	    res[i] = q.getCompletionInfo( jobs, workers );
	}
	return res;
    }
}
