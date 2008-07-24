package ibis.maestro;

import ibis.maestro.LabelTracker.Label;

import java.io.Serializable;

class MapReduceHandler implements CompletionListener
{
	final Node node;
	final MapReduceTask reducer;
	final LabelTracker labeler = new LabelTracker();

	/**
	 * @param node
	 * @param reducer
	 */
	MapReduceHandler( Node node, MapReduceTask reducer )
	{
		this.node = node;
		this.reducer = reducer;
	}
	
	private static final class Id implements Serializable
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		final Object userID;
		final Label label;
		public Id(final Object userID, final Label label) {
			super();
			this.userID = userID;
			this.label = label;
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
	public synchronized void submit( Job job, Object input, Object userId )
	{
		Label label = labeler.nextLabel();
		Object id = new Id( userId, label );
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
	protected Object waitForResult() {
		WorkThread worker = node.spawnExtraWorker();
		labeler.waitForAllLabels();
		node.stopWorker( worker );
		return reducer.getResult();
	}

}
