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

        WaiterTaskIdentifier( int id )
        {
            this.id = id;
        }
    }

    /** Submits the given job to the given node.
     * Internally we keep track of the number of submitted jobs so that
     * we can wait for all of them to complete.
     * 
     * @param node The node to submit the job to.
     * @param j The job to submit.
     */
    public synchronized void submit( Task task, Job j )
    {
	Object id = new WaiterTaskIdentifier( jobNo++ );
        outstandingJobs++;
        task.submit( j, id, this );
    }

    /**
     * 
     * @param node The node we're running on.
     * @param userId The identifier of the task that was completed.
     * @param result The result of the task.
     */
    @Override
    public synchronized void jobCompleted( Node node, Object userId, Object result )
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
     * @param node The node this waiter runs on.
     * @return The array of all reported job results.
     */
    public Object[] sync( Node node )
    {
        Object res[];
        WorkThread thread = node.startExtraWorker();
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
        thread.shutdown();
        return res;
    }
}
