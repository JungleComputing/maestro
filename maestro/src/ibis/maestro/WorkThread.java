package ibis.maestro;

/**
 * A single worker thread of a Maestro worker.
 *
 * @author Kees van Reeuwijk.
 */
final class WorkThread extends Thread
{
    private final TaskSource source;
    private final Node localNode;
    private boolean stopped = false;

    /**
     * Given a work source, constructs a new WorkThread.
     * @param source The work source.
     * @param localNode The local node.
     */
    WorkThread( TaskSource source, Node localNode )
    {
        super( "Work thread" );
        this.source = source;
        this.localNode = localNode;
    }

    private synchronized boolean isStopped()
    {
	return stopped;
    }

    /**
     * Run this thread: keep getting and executing tasks until a null
     * task is returned.
     */
    @Override
    public void run()
    {
        while( !isStopped() ) {
            RunTask runTask = source.getTask();
            Object result;

            if( runTask == null ) {
                break;
            }
            if( Settings.traceWorkerProgress ) {
        	System.out.println( "Work thread: executing " + runTask.message );
            }
            Task task = runTask.task;
            if( task instanceof AtomicTask ) {
        	AtomicTask at = (AtomicTask) task;
        	result = at.run( runTask.message.task.input, localNode );
            }
            else if( task instanceof MapReduceTask ) {
        	MapReduceTask mrt = (MapReduceTask) task;
        	MapReduceHandler handler = new MapReduceHandler( localNode, mrt );
        	// FIXME: start a new work thread to compensate for this one.
        	result = handler.waitForResult();
            }
            else {
        	Globals.log.reportInternalError( "Don't know what to do with a task of type " + task.getClass() );
        	result = null;
            }
            if( Settings.traceWorkerProgress ) {
        	System.out.println( "Work thread: completed " + runTask.message );
            }
            source.reportTaskCompletion( runTask, result );
        }
    }

    /** Tell this work thread to shut down. We don't wait for
     * it to stop, since it won't run a new task in any case.
     */
    void shutdown()
    {
	synchronized( this ) {
	    stopped = true;
	}
    }
}
