package ibis.maestro;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    private static final int JOBCOUNT = 500;

    private class Listener implements CompletionListener
    {

	/** Handle the completion of job 'j': the result is 'result'.
	 * @param j The job that was completed.
	 * @param result The result of the job.
	 */
	@Override
	public void jobCompleted(Job j, JobReturn result ) {
	    //System.out.println( "Job " + j + ": result is " + result );
	}
	
    }

    @SuppressWarnings("synthetic-access")
    private void run( boolean goForMaestro ) throws Exception
    {
        Node node = new Node( new Listener(), goForMaestro );

        if( node.isMaestro() ) {
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
	System.out.println( "Running on platform " + Service.getPlatformVersion() + " args.length=" + args.length );
        boolean goForMaestro = args.length == 0;
	try {
            new TestProg().run( goForMaestro );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
