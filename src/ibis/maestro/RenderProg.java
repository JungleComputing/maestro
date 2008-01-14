package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;

/**
 * Test the master/worker framework by submitting render jobs.
 * @author Kees van Reeuwijk
 *
 */
public class RenderProg {
    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.ELECTIONS_STRICT );

    private Master master;

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
    private void startMaster( Ibis myIbis ) throws IOException {
	master = new Master( myIbis, new Listener() );
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
        Ibis ibis = IbisFactory.createIbis( ibisCapabilities, null, PacketSendPort.portType, PacketBlockingReceivePort.portType, PacketUpcallReceivePort.portType );

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
