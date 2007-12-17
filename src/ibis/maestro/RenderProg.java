package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;

/**
 * Test the master/worker framework by submitting render jobs.
 * @author Kees van Reeuwijk
 *
 */
public class RenderProg {
    private PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_EXPLICIT, PortType.CONNECTION_ONE_TO_ONE );

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
    private void startMaster( Ibis myIbis ) throws IOException {
	master = new Master<Double>( myIbis, new Listener() );
    }

    private void startWorker( Ibis myIbis, IbisIdentifier server ) throws IOException {
	ReceivePortIdentifier masterPort = null; // FIXME: 
	Worker<MultiplyJob> worker = new Worker<MultiplyJob>( myIbis, masterPort );
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
        Ibis ibis = IbisFactory.createIbis( ibisCapabilities, null, portType );

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
    public static void main( String args[] ){
        try {
            new RenderProg().run();            
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
