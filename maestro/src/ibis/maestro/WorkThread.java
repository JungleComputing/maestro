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
            RunTask task = source.getTask();

            if( task == null ) {
                break;
            }
            if( Settings.traceWorkerProgress ) {
        	System.out.println( "Work thread: executing " + task.message );
            }
            Object result = task.task.run( task.message.task.input, localNode );
            if( Settings.traceWorkerProgress ) {
        	System.out.println( "Work thread: completed " + task.message );
            }
            source.reportTaskCompletion( task, result );
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
