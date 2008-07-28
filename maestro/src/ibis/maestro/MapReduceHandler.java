package ibis.maestro;

import ibis.maestro.LabelTracker.Label;

import java.io.Serializable;

/**
 * Handles the execution of a map/reduce task.
 * In particular, it handles the submission of sub-tasks,
 * waits for their completion, applies the reduction,
 * and handles the result.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class MapReduceHandler extends Thread implements CompletionListener
{
    final Node localNode;
    final MapReduceTask reducer;
    final LabelTracker labeler = new LabelTracker();
    final RunTaskMessage message;
    final long runMoment;

    /**
     * @param localNode The node we are running on.
     * @param reducer The reducer to run on each result we're waiting for.
     */
    MapReduceHandler( Node localNode, MapReduceTask reducer, RunTaskMessage message, long runMoment )
    {
	this.localNode = localNode;
	this.reducer = reducer;
	this.message = message;
	this.runMoment = runMoment;
    }

    private static final class Id implements Serializable
    {
	private static final long serialVersionUID = 1L;

	final Object userID;
	final Label label;

	private Id( final Object userID, final Label label ) {
	    super();
	    this.userID = userID;
	    this.label = label;
	}

	/**
	 * @return A string representation of this id.
	 */
	@Override
	public String toString()
	{
	    return "label=" + label + " userID=" + userID;
	}
    }

    /** Submits a new job instance with the given input for the first task of the job.
     * Internally we keep track of the number of submitted jobs so that
     * we can wait for all of them to complete.
     * 
     * @param job The job to submit to.
     * @param input Input for the (first task of the) job.
     * @param userId The identifier the user attaches to this job.
     */
    @SuppressWarnings("synthetic-access")
    public synchronized void submit( Job job, Object input, Object userId )
    {
	Label label = labeler.nextLabel();
	Object id = new Id( userId, label );
	if( Settings.traceMapReduce ){
	    Globals.log.reportProgress( "MapReduce: Submitting " + id + " to " + job );
	}
	job.submit( localNode, input, id, this );
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
	if( Settings.traceMapReduce ){
	    Globals.log.reportProgress( "MapReduce: got back " + userId );
	}
	if( !(userId instanceof Id) ){
	    Globals.log.reportInternalError( "The identifier is not a MapReduceHandler.Id but a " + userId.getClass() );
	    return;
	}
	Id id = (Id) userId;
	labeler.returnLabel( id.label );
	reducer.reduce( id.userID, result );
    }

    /**
     * Runs this thread. We assume that the map phase has been completed,
     * so all we have to do is wait for the return of all results.
     */
    @Override
    public void run()
    {
	try {
	    labeler.waitForAllLabels();
	    // FIXME: this blocks a worker thread!!!
	}
	catch( InterruptedException x ) {
	    // Nothing we can do.
	}
	Object result = reducer.getResult();
	localNode.transferResult( message, result, runMoment );
    }

}
