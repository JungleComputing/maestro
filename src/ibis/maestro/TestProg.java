package ibis.maestro;

import ibis.server.Server;

import java.util.Properties;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    private Node node;
    private static final int JOBCOUNT = 12;

    private class Listener implements CompletionListener {

	/** Handle the completion of job 'j': the result is 'result'.
	 * @param j The job that was completed.
	 * @param result The result of the job.
	 */
	@Override
	public void jobCompleted(Job j, JobReturn result ) {
	    System.out.println( "Job " + j + ": result is " + result );
	}
	
    }

    private void submitJob( int n )
    {
	MultiplyJob j = new MultiplyJob( n );
	node.submit( j );
    }

    @SuppressWarnings("synthetic-access")
    private void run() throws Exception {
        // Create an ibis instance.
        Properties serverProperties = new Properties();
        //serverProperties.setProperty( "ibis.server.port", "12642" );
        Server ibisServer = new Server( serverProperties );
        String serveraddress = ibisServer.getLocalAddress();
        node = new Node( serveraddress, new Listener() );

        for( int i=0; i<JOBCOUNT; i++ ){
            submitJob( 1000*i );
        }
        node.finish();
        Globals.tracer.close();
        System.out.println( "Test program has ended" );
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] ) {
	System.out.println( "Running on platform " + Service.getPlatformVersion() );
	try {
            new TestProg().run();            
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
