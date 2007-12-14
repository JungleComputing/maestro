package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_EXPLICIT, PortType.CONNECTION_ONE_TO_ONE );

    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.ELECTIONS_STRICT );

    private Master<MultiplyJob> master;

    private void startMaster( Ibis myIbis ) throws IOException {
	master = new Master<MultiplyJob>( myIbis );
    }

    private void startWorker( Ibis myIbis, IbisIdentifier server ) throws IOException {
	ReceivePortIdentifier masterPort = null; // FIXME: 
	Worker<MultiplyJob> worker = new Worker<MultiplyJob>( myIbis, masterPort );
	worker.run();
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
        
        ibis.end();
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] ){
        try {
            new TestProg().run();            
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
