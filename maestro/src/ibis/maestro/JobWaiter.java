package ibis.maestro;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Waits for the given job.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class JobWaiter implements CompletionListener
{
    private int taskNo = 0;
    private int outstandingTasks = 0;
    private ArrayList<Object> results = new ArrayList<Object>();

    private static class WaiterJobIdentifier implements Serializable {
        private static final long serialVersionUID = -3256737277889247302L;
        final int id;

        private WaiterJobIdentifier( int id )
        {
            this.id = id;
        }
    }

    /** Submits a new job instance with the given input for the first task of the job.
     * Internally we keep track of the number of submitted jobs so that
     * we can wait for all of them to complete.
     * 
     * @param node The node this should run on.
     * @param job The job to submit to.
     * @param input Input for the (first task of the) job.
     */
    @SuppressWarnings("synthetic-access")
    public synchronized void submit( Node node, Job job, Object input )
    {
	Object id = new WaiterJobIdentifier( taskNo++ );
        outstandingTasks++;
        job.submit( node, input, id, this );
    }

    /**
     * Handle the completion of a task. We do this by storing the result
     * in a local array.
     * @param node The node we're running on.
     * @param userId The identifier of the job that was completed.
     * @param result The result of the job.
     */
    @Override
    public synchronized void jobCompleted( Node node, Object userId, Object result )
    {
        if( userId instanceof WaiterJobIdentifier ){
            int ix = ((WaiterJobIdentifier) userId).id;
            while( results.size()<=ix ){
                results.add( null );
            }
            results.set( ix, result );
            outstandingTasks--;
        }
        else {
            System.err.println( "TaskWaiter: don't know what to do with user identifier " + userId );
        }
        notifyAll();
    }

    /**
     * Wait for all tasks to be completed.
     * @return The array of all reported task results.
     */
    public Object[] sync()
    {
        Object res[];
        while( true ) {
            synchronized( this ){
                if( outstandingTasks == 0 ){
                    res = new Object[results.size()];
                    results.toArray( res );

                    // Prepare for a possible new round.
                    results.clear();
                    outstandingTasks = 0;
                    taskNo = 0;
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
