package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.RegistryEventHandler;

import java.io.IOException;
import java.util.Properties;

/**
 * A node in the Maestro dataflow network.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Node extends Thread {
    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.ELECTIONS_STRICT );

    private final Ibis ibis;
    private final Master master;
    private final Worker worker;

    private class MaestroRegistryEventHandler implements RegistryEventHandler {
    
	/**
	 * An ibis has died.
	 * @param theIbis The ibis that died.
	 */
        @Override
        public void died(IbisIdentifier theIbis) {
            worker.removeIbis( theIbis );
            master.removeIbis( theIbis );
        }
    
        /**
         * The results of an election are known.
         * @param arg0 The name of the election.
         * @param arg1 The ibis that was elected.
         */
        @Override
        public void electionResult(String arg0, IbisIdentifier arg1) {
            // Not interested.
        }
    
        /**
         * Our ibis got a signal.
         * @param arg0 The signal.
         */
        @Override
        public void gotSignal(String arg0) {
            // Not interested.
        }
    
        /**
         * A new Ibis joined the computation.
         * @param theIbis The ibis that joined the computation.
         */
        @Override
        public void joined(IbisIdentifier theIbis) {
            master.addIbis( theIbis );
            worker.addIbis( theIbis );
        }
    
        /**
         * An ibis has explicitly left the computation.
         * @param theIbis The ibis that left.
         */
        @Override
        public void left(IbisIdentifier theIbis) {
            worker.removeIbis( theIbis );
            master.removeIbis( theIbis );
        }
    }

    /**
     * Constructs a new Maestro node using the given name server and completion listener.
     * @param serverAddress The name server to use.
     * @param listener A completion listener for computations completed by this node.
     * @throws IbisCreationFailedException
     * @throws IOException
     */
    public Node( String serverAddress, CompletionListener listener ) throws IbisCreationFailedException, IOException
    {
	Properties ibisProperties = new Properties();
	RegistryEventHandler registryEventHandler = new MaestroRegistryEventHandler();
	ibisProperties.setProperty( "ibis.server.address", serverAddress );
	ibisProperties.setProperty( "ibis.pool.name", "MaestroPool" );
	ibis = IbisFactory.createIbis(
	    ibisCapabilities,
	    ibisProperties,
	    true,
	    registryEventHandler,
	    PacketSendPort.portType,
	    PacketUpcallReceivePort.portType,
	    PacketBlockingReceivePort.portType
	);
	master = new Master( ibis, listener );
	master.start();
	worker = new Worker( ibis, master );
	worker.start();
	if( Settings.traceNodes ) {
	    Globals.log.log( "Started a Maestro node. serverAddress=" + serverAddress );
	}
    }
    
    /** Submits the given job to this node.
     * @param j The job to submit.
     */
    public void submit( Job j )
    {
	master.submit( j );
    }

    public void run()
    {
	try {
	    ibis.end();
	}
	catch( Exception x ) {
	    x.printStackTrace();
	}
    }
}
