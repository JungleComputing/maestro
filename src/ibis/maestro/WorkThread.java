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

    /**
     * Run this thread: keep getting and executing jobs until a null
     * job is returned.
     */
    @Override
    public void run()
    {
        while( true ) {
            RunJobMessage job = source.getJob();
            
            if( job == null ) {
                break;
            }
            job.job.run( localNode, job.taskIdentifier );
            source.reportJobCompletion( job );
        }
    }
}
