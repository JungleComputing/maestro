package ibis.maestro;

class MapReduceHandler implements CompletionListener
{
    final Node node;
    final MapReduceTask reducer;

    /**
     * @param node
     * @param reducer
     */
    MapReduceHandler( Node node, MapReduceTask reducer )
    {
	this.node = node;
	this.reducer = reducer;
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
    public synchronized void submit( Job job, Object input )
    {
	Object userId = null; // FIXME:!!!
	job.submit( node, input, userId, this );
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
    }

    /**
     * Wait until all jobs have been executed, and then ask the reducer for the
     * result.
     * @return The result as reported by the reducer.
     */
    protected Object waitForResult() {
	// TODO: Auto-generated method stub
	return null;
    }

}
