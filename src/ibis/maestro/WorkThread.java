package ibis.maestro;

/**
 * A single worker thread of a Maestro worker.
 *
 * @author Kees van Reeuwijk.
 */
class WorkThread extends Thread
{
    private final WorkSource source;
    private final Node localNode;
    private boolean stopped = false;

    /**
     * Given a work source, constructs a new WorkThread.
     * @param source The work source.
     * @param localMaster The local master.
     */
    WorkThread( WorkSource source, Node localMaster )
    {
        super( "Work thread" );
        setPriority( NORM_PRIORITY+1 );
        this.source = source;
        this.localNode = localMaster;
    }
    
    private synchronized boolean isStopped()
    {
	return stopped;
    }

    /**
     * Run this thread: keep getting and executing jobs until a null
     * job is returned.
     */
    @Override
    public void run()
    {
        while( !isStopped() ) {
            RunJobMessage job = source.getJob();
            
            if( job == null ) {
                break;
            }
            job.job.run( localNode, job.taskIdentifier );
            source.reportJobCompletion( job );
        }
    }

    /** Tell this work thread to shut down, and wait for it to do so. */
    public void shutdown()
    {
	synchronized( this ) {
	    stopped = true;
	}
	Service.waitToTerminate( this );
    }
}
