package ibis.maestro;

import java.util.ArrayList;

import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.CompletionListener;
import ibis.maestro.Job;
import ibis.maestro.JobResultValue;
import ibis.maestro.Node;
import ibis.maestro.TaskIdentifier;

/**
 * Waits for the given set of jobs.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class JobWaiter implements CompletionListener {
    private int jobNo = 0;
    private int outstandingJobs = 0;
    private ArrayList<JobResultValue> results = new ArrayList<JobResultValue>();

    private static class WaiterTaskIdentifier implements TaskIdentifier {
        private static final long serialVersionUID = -3256737277889247302L;
        final ReceivePortIdentifier resultPort;
        final int id;

        WaiterTaskIdentifier( int id, ReceivePortIdentifier resultPort )
        {
            this.id = id;
            this.resultPort = resultPort;
        }

        /**
         * Reports the result back to the original submitter of the task
         * by sending a result message to the port we're carrying along.
         * @param node The node we're running on.
         * @param result The result to report.
         */
	@Override
	public void reportResult( Node node, JobResultValue result )
	{
	    node.sendResultMessage( resultPort, this, result );
	}
    }

    /** Submits the given job to the given node.
     * Internally we keep track of the number of submitted jobs so that
     * we can wait for all of them to complete.
     * 
     * @param node The node to submit the job to.
     * @param j The job to submit.
     */
    public synchronized void submit( Node node, Job j )
    {
	TaskIdentifier id = node.buildTaskIdentifier( jobNo++ );
        outstandingJobs++;
        node.submitTask( j, this, id );
    }

    /**
     * 
     * @param node The node we're running on.
     * @param id The identifier of the task that was completed.
     * @param result The result of the task.
     */
    @Override
    public synchronized void jobCompleted( Node node, TaskIdentifier id, JobResultValue result )
    {
        int ix = ((WaiterTaskIdentifier) id).id;
        results.set( ix, result );
        outstandingJobs--;
        notifyAll();
    }

    /**
     * Wait for all jobs to be completed.
     * @param node The node this waiter runs on.
     * @return The array of all reported job results.
     */
    public JobResultValue[] sync( Node node )
    {
        JobResultValue res[];
        WorkThread thread = node.startExtraWorker();
        while( true ) {
            synchronized( this ){
                if( outstandingJobs == 0 ){
                    res = new JobResultValue[results.size()];
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
