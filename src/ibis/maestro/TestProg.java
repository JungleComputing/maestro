package ibis.maestro;

import ibis.server.Server;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    private static final int JOBCOUNT = 1000;

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
    
    private Server ibisServer = null;

    @SuppressWarnings("synthetic-access")
    private void run() throws Exception
    {
        Node node = new Node( new Listener() );

        if( node.isMaestro() ) {
            for( int i=0; i<JOBCOUNT; i++ ){
        	AdditionJob j = new AdditionJob( 12*i );
        	node.submit( j );
            }
            node.finish();
        }
        Globals.tracer.close();
        if( ibisServer != null ){
            ibisServer.end( true );
        }
        System.out.println( "Test program has ended" );
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] )
    {
	System.out.println( "Running on platform " + Service.getPlatformVersion() );
	try {
            new TestProg().run();            
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
