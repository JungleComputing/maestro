package ibis.maestro;

/**
 * A single worker thread of a Maestro worker.
 *
 * @author Kees van Reeuwijk.
 */
class WorkThread extends Thread
{
    private final WorkSource source;
    private final Master localMaster;

    /**
     * Given a work source, constructs a new WorkThread.
     * @param source The work source.
     * @param localMaster The local master.
     */
    WorkThread( WorkSource source, Master localMaster )
    {
        super( "Work thread" );
        this.source = source;
        this.localMaster = localMaster;
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
            JobReturn r = job.getJob().run( localMaster );
            source.reportJobResult( job, r );
        }
        System.out.println( "Work thread terminated" );
    }
    
    
}
