package ibis.maestro;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Waits for the given task.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class TaskWaiter implements CompletionListener
{
    private int jobNo = 0;
    private int outstandingJobs = 0;
    private ArrayList<Object> results = new ArrayList<Object>();

    private static class WaiterTaskIdentifier implements Serializable {
        private static final long serialVersionUID = -3256737277889247302L;
        final int id;

        private WaiterTaskIdentifier( int id )
        {
            this.id = id;
        }
    }

    /** Submits a new task instance with the given input for the first job of the task.
     * Internally we keep track of the number of submitted tasks so that
     * we can wait for all of them to complete.
     * 
     * @param task The task to submit to.
     * @param input Input for the (first job of the) task.
     */
    @SuppressWarnings("synthetic-access")
    public synchronized void submit( Task task, Object input )
    {
	Object id = new WaiterTaskIdentifier( jobNo++ );
        outstandingJobs++;
        task.submit( input, id, this );
    }

    /**
     * Handle the completion of a job. We do this by storing the result
     * in a local array.
     * @param node The node we're running on.
     * @param userId The identifier of the task that was completed.
     * @param result The result of the task.
     */
    @Override
    public synchronized void taskCompleted( Node node, Object userId, Object result )
    {
        if( userId instanceof WaiterTaskIdentifier ){
            int ix = ((WaiterTaskIdentifier) userId).id;
            while( results.size()<=ix ){
                results.add( null );
            }
            results.set( ix, result );
            outstandingJobs--;
        }
        else {
            System.err.println( "JobWaiter: don't know what to do with user identifier " + userId );
        }
        notifyAll();
    }

    /**
     * Wait for all jobs to be completed.
     * @return The array of all reported job results.
     */
    public Object[] sync()
    {
        Object res[];
        while( true ) {
            synchronized( this ){
                if( outstandingJobs == 0 ){
                    res = new Object[results.size()];
                    results.toArray( res );

                    // Prepare for a possible new round.
                    results.clear();
                    outstandingJobs = 0;
                    jobNo = 0;
                    break;
                }
                try {
                    wait();
                } catch (InterruptedException e) {
                    // Not interesting.
                }
            }
        }
        return res;
    }
}
