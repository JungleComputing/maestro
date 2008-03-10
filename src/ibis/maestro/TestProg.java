package ibis.maestro;

import java.util.ArrayList;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    private static class Listener implements CompletionListener
    {
        int jobsCompleted = 0;
        private final int jobCount;

        Listener( int jobCount )
        {
            this.jobCount = jobCount;
        }

        /** Handle the completion of job 'j': the result is 'result'.
	 * @param id The job that was completed.
	 * @param result The result of the job.
	 */
	@Override
	public void jobCompleted( Node node, long id, JobProgressValue result ) {
	    //System.out.println( "result is " + result );
            jobsCompleted++;
            if( jobsCompleted>=jobCount ){
                node.setStopped();
            }
	}
    }

    @SuppressWarnings("synthetic-access")
    private void run( int jobCount, boolean goForMaestro ) throws Exception
    {
	ArrayList<JobType> allowedTypes = new ArrayList<JobType>();
	
	allowedTypes.add( AdditionJob.jobType );
        Node node = new Node( new Listener( jobCount ), allowedTypes, goForMaestro );

	System.out.println( "Node created" );
        if( node.isMaestro() ) {
            System.out.println( "I am maestro; submitting " + jobCount + " jobs" );
            for( int i=0; i<jobCount; i++ ){
        	ReportReceiver watcher = node.createReportReceiver( i );
		AdditionJob j = new AdditionJob( 12*i, watcher  );
        	node.submit( j );
            }
        }
        node.waitToTerminate();
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] )
    {
        boolean goForMaestro = true;
        int jobCount = 0;

        if( args.length == 0 ){
            System.err.println( "Missing parameter: I need a job count, or 'worker'" );
            System.exit( 1 );
        }
        String arg = args[0];
        if( arg.equalsIgnoreCase( "worker" ) ){
            goForMaestro = false;
        }
        else {
            jobCount = Integer.parseInt( arg );
        }
	System.out.println( "Running on platform " + Service.getPlatformVersion() + " args.length=" + args.length + " goForMaestro=" + goForMaestro + "; jobCount=" + jobCount );
	try {
            new TestProg().run( jobCount, goForMaestro );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
