package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.RegistryEventHandler;

import java.io.IOException;
import java.util.Properties;
import java.util.Vector;

/**
 * A node in the Maestro dataflow network.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Node implements RegistryEventHandler {
    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.MEMBERSHIP_UNRELIABLE, IbisCapabilities.ELECTIONS_STRICT );
    private final Ibis ibis;
    private final Master master;
    private final Worker worker;
    
    /** The list of maestro nodes in this computation. */
    private Vector<MaestroInfo> maestros = new Vector<MaestroInfo>();
    private boolean isMaestro;

    /** The list of maestros in this computation. */
    private static class MaestroInfo {
	IbisIdentifier ibis;   // The identifier of the maestro.
	boolean seen;          // Did we already see this maestro?

	MaestroInfo(IbisIdentifier id ) {
	    this.ibis = id;
	}
    }

    /**
     * Returns true iff this node is a maestro.
     * @return True iff this node is a maestro.
     */
    public boolean isMaestro() { return isMaestro; }

    /**
     * Registers the ibis with the given identifier as one that has joined the
     * computation.
     * @param id The identifier of the ibis.
     */
    private void registerIbisJoined( IbisIdentifier id )
    {
	for( MaestroInfo m: maestros ) {
	    if( m.ibis.equals( id ) ) {
		m.seen = true;
	    }
	}
    }

    /** Registers the ibis with the given identifier as one that has left the
     * computation.
     * @param id The ibis that has left.
     */
    private void registerIbisLeft( IbisIdentifier id )
    {
	int ix = maestros.size();

	while( ix>0 ) {
	    MaestroInfo m = maestros.get(ix);
	    if( m.ibis.equals( id )) {
		maestros.remove(ix);
	    }
	}
	if( maestros.size() == 0 ) {
	    // Everyone has left, we might as well stop.
	    finish();
	}
    }

    /**
     * A new Ibis joined the computation.
     * @param theIbis The ibis that joined the computation.
     */
    @Override
    public void joined( IbisIdentifier theIbis )
    {
        registerIbisJoined( theIbis );
        master.addIbis( theIbis );
        worker.addIbis( theIbis );
    }

    /**
     * An ibis has died.
     * @param theIbis The ibis that died.
     */
    @Override
    public void died( IbisIdentifier theIbis )
    {
	registerIbisLeft( theIbis );
        worker.removeIbis( theIbis );
        master.removeIbis( theIbis );
    }

    /**
     * An ibis has explicitly left the computation.
     * @param theIbis The ibis that left.
     */
    @Override
    public void left( IbisIdentifier theIbis )
    {
        registerIbisLeft( theIbis );
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
     * Constructs a new Maestro node using the given name server and completion listener.
     * @param listener A completion listener for computations completed by this node.
     * @throws IbisCreationFailedException Thrown if for some reason we cannot create an ibis.
     * @throws IOException Thrown if for some reason we cannot communicate.
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
	IbisIdentifier maestro = ibis.registry().elect( "maestro" );
	isMaestro = maestro.equals( ibis.identifier() );
	this.maestros.add( new MaestroInfo( maestro ) );
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
