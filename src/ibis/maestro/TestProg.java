package ibis.maestro;

import java.util.ArrayList;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    private static final int JOBCOUNT = 100;

    private class Listener implements CompletionListener
    {

	/** Handle the completion of job 'j': the result is 'result'.
	 * @param j The job that was completed.
	 * @param result The result of the job.
	 */
	@Override
	public void jobCompleted(Job j, JobProgressValue result ) {
	    //System.out.println( "Job " + j + ": result is " + result );
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
        	AdditionJob j = new AdditionJob( 12*i );
        	node.submit( j );
            }
            node.finish();
        }
        else {
            node.waitToTerminate();
        }
        Globals.tracer.close();
        System.out.println( "Test program has ended" );
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
