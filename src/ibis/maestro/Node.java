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
public class Node implements RegistryEventHandler {
    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.MEMBERSHIP_UNRELIABLE );
    private final Ibis ibis;
    private final Master master;
    private final Worker worker;

    /**
     * An ibis has died.
     * @param theIbis The ibis that died.
     */
    @Override
    public void died( IbisIdentifier theIbis )
    {
        worker.removeIbis( theIbis );
        master.removeIbis( theIbis );
    }

    /**
     * The results of an election are known.
     * @param arg0 The name of the election.
     * @param arg1 The ibis that was elected.
     */
    @Override
    public void electionResult( String arg0, IbisIdentifier arg1 )
    {
        // Not interested.
    }

    /**
     * Our ibis got a signal.
     * @param arg0 The signal.
     */
    @Override
    public void gotSignal( String arg0 )
    {
        // Not interested.
    }

    /**
     * A new Ibis joined the computation.
     * @param theIbis The ibis that joined the computation.
     */
    @Override
    public void joined( IbisIdentifier theIbis )
    {
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

    /**
     * Constructs a new Maestro node using the given name server and completion listener.
     * @param listener A completion listener for computations completed by this node.
     * @throws IbisCreationFailedException
     * @throws IOException
     */
    public Node( CompletionListener listener ) throws IbisCreationFailedException, IOException
    {
	Properties ibisProperties = new Properties();
	ibisProperties.setProperty( "ibis.pool.name", "MaestroPool" );
	ibis = IbisFactory.createIbis(
	    ibisCapabilities,
	    ibisProperties,
	    true,
	    this,
	    PacketSendPort.portType,
	    PacketUpcallReceivePort.portType,
	    PacketBlockingReceivePort.portType
	);
	master = new Master( ibis, listener );
	master.start();
        worker = new Worker( ibis, master );
        worker.start();
        master.waitForSubscription(  worker.identifier() );
	if( Settings.traceNodes ) {
	    Globals.log.log( "Started a Maestro node" );
	}
    }
    
    /** Submits the given job to this node.
     * @param j The job to submit.
     */
    public void submit( Job j )
    {
	master.submit( j );
    }

    /** Set this node to a stopped mode. */
    public void setStopped()
    {
        master.setStopped();
    }
    
    /** Finish this node. */
    public void finish()
    {
        master.setStopped();
        /**
         * Everything interesting happens in the master and worker.
         * So all we do here is wait for the master and worker to terminate.
         * We only stop this thread if both are terminated, so we can just wait
         * for one to terminate, and then the other.
         */
        Service.waitToTerminate( master );
        System.out.println( "Node master has terminated; terminating worker" );
        worker.setStopped();
        Service.waitToTerminate( worker );
        try {
            ibis.end();
        }
        catch( IOException x ) {
            // Nothing we can do about it.
        }
        System.out.println( "Node has terminated" );
    }
}
