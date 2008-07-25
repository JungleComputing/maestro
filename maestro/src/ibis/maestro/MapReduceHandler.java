package ibis.maestro;

import ibis.maestro.LabelTracker.Label;

import java.io.Serializable;

public class MapReduceHandler implements CompletionListener
{
    final Node localNode;
    final MapReduceTask reducer;
    final LabelTracker labeler = new LabelTracker();

    /**
     * @param node
     * @param reducer
     */
    MapReduceHandler( Node node, MapReduceTask reducer )
    {
	this.localNode = node;
	this.reducer = reducer;
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
     * Wait until all jobs have been executed, and then ask the reducer for the
     * result.
     * @return The result as reported by the reducer.
     */
    protected Object waitForResult()
    {
	WorkThread worker = null;

	try {
	    worker = localNode.spawnExtraWorker();
	    labeler.waitForAllLabels();
	}
	catch( InterruptedException x ) {
	    return null;
	}
	finally {
	    if( worker != null ) {
		localNode.stopWorker( worker );
	    }
	}
	return reducer.getResult();
    }

}
