package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.server.Server;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.ELECTIONS_STRICT );

    private Master<Double> master;

    private class Listener implements CompletionListener<Double> {

	/** Handle the competion of job 'j': the result is 'result'.
	 * @param j The job that was completed.
	 * @param result The result of the job.
	 */
	@Override
	public void jobCompleted(Job<Double> j, Double result) {
	    System.out.println( "Job " + j + ": result is " + result );
	}
	
    }
    @SuppressWarnings("synthetic-access")
    private void startMaster( Ibis myIbis ) throws Exception {
	master = new Master<Double>( myIbis, new Listener() );
    }

    private void startWorker( Ibis myIbis, IbisIdentifier server ) throws IOException {
	Worker<MultiplyJob> worker = new Worker<MultiplyJob>( myIbis, server );
	worker.run();
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
        IbisIdentifier server = ibis.registry().elect("Server");
        
        // If I am the server, run server, else run client.
        if( server.equals( ibis.identifier())){
            startMaster(ibis);
        }
        startWorker(ibis,server);

        
        submitJob( buildSeries( 3 ) );
        submitJob( buildSeries( 12 ) );
        ibis.end();
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] ) throws Exception {
        String ibis_home = System.getenv( "IBIS_HOME");
        if( ibis_home == null ){
            System.err.println( "Environment variable IBIS_HOME is not set" );
        }
        else {
            File f = new File( ibis_home );
            if( !f.exists() ){
                System.err.println( "The specified IBIS_HOME [" + ibis_home + "] does not exist" );
            }
            else if( !f.isDirectory() ){
                System.err.println( "The specified IBIS_HOME [" + ibis_home + "] is not a directory" );
            }
        }
        try {
            new TestProg().run();            
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
