package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.Registry;
import ibis.server.Server;

import java.io.IOException;
import java.util.Properties;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.ELECTIONS_STRICT );

    private SendQueue master;

    private class Listener implements CompletionListener {

	/** Handle the competion of job 'j': the result is 'result'.
	 * @param j The job that was completed.
	 * @param result The result of the job.
	 */
	@Override
	public void jobCompleted(Job j, JobReturn result) {
	    System.out.println( "Job " + j + ": result is " + result );
	}
	
    }
    @SuppressWarnings("synthetic-access")
    private void startMaster( Ibis myIbis ) throws Exception {
	master = new SendQueue( myIbis, new Listener() );
    }

    private void startWorker( Ibis myIbis ) throws IOException {
	Worker worker = new Worker( myIbis );
	worker.run();
        worker.getJobPort();
    }
    
    private void submitJob( double [] arr )
    {
	MultiplyJob j = new MultiplyJob( arr );
	master.submit( j );
    }
    
    private double [] buildSeries( int n )
    {
	double res[] = new double[n];
	
	for( int i=0; i<n; i++ ) {
	    res[i] = i+1;
	}
	return res;
    }

    private void run() throws Exception {
        // Create an ibis instance.
        Properties serverProperties = new Properties();
        //serverProperties.setProperty( "ibis.server.port", "12642" );
        Server ibisServer = new Server( serverProperties );
        String serveraddress = ibisServer.getLocalAddress();
        Properties ibisProperties = new Properties();
        ibisProperties.setProperty( "ibis.server.address", serveraddress );
        ibisProperties.setProperty( "ibis.pool.name", "XXXpoolname" );
        Ibis ibis = IbisFactory.createIbis(ibisCapabilities, ibisProperties, true, null, PacketSendPort.portType, PacketUpcallReceivePort.portType, PacketBlockingReceivePort.portType );

        // Elect a server
        Registry registry = ibis.registry();
	IbisIdentifier server = registry.elect("Server");
        
        // If I am the server, run server, else run client.
        if( server.equals( ibis.identifier())){
            startMaster(ibis);
            
            submitJob( buildSeries( 3 ) );
            submitJob( buildSeries( 12 ) );
        }
        startWorker(ibis );

        ibis.end();
        System.out.println( "Test program has ended" );
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] ) {
        try {
            new TestProg().run();            
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
