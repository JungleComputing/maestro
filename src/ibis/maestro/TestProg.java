package ibis.maestro;

import java.util.ArrayList;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    private static final int JOBCOUNT = 30000;

    private static class Listener implements CompletionListener
    {
        int jobsCompleted = 0;

	/** Handle the completion of job 'j': the result is 'result'.
	 * @param id The job that was completed.
	 * @param result The result of the job.
	 */
	@Override
	public void jobCompleted( Node node, long id, JobProgressValue result ) {
	    //System.out.println( "result is " + result );
            jobsCompleted++;
            if( jobsCompleted>=JOBCOUNT ){
                node.setStopped();
            }
	}
	
    }

    @SuppressWarnings("synthetic-access")
    private void run( boolean goForMaestro ) throws Exception
    {
	ArrayList<JobType> allowedTypes = new ArrayList<JobType>();
	
	allowedTypes.add( AdditionJob.jobType );
        Node node = new Node( new Listener(), allowedTypes, goForMaestro );

	System.out.println( "Node created" );
        if( node.isMaestro() ) {
            System.out.println( "I am maestro; submitting " + JOBCOUNT + " jobs" );
            for( int i=0; i<JOBCOUNT; i++ ){
        	ReportReceiver watcher = node.createReportReceiver( i );
		AdditionJob j = new AdditionJob( 12*i, watcher  );
        	node.submit( j );
            }
        }
        node.waitToTerminate();
        Globals.tracer.close();
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] )
    {
        boolean goForMaestro = args.length == 0;
	System.out.println( "Running on platform " + Service.getPlatformVersion() + " args.length=" + args.length + " goForMaestro=" + goForMaestro );
	try {
            new TestProg().run( goForMaestro );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
