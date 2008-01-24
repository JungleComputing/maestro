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
public class Node extends Thread implements RegistryEventHandler {
    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.MEMBERSHIP_UNRELIABLE );
    private final Ibis ibis;
    private final Master master;
    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    private final Worker workers[] = new Worker[numberOfProcessors];

    /**
     * An ibis has died.
     * @param theIbis The ibis that died.
     */
    @Override
    public void died(IbisIdentifier theIbis) {
        for( Worker w: workers ){
            w.removeIbis( theIbis );
        }
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
        for( Worker w: workers ){
            w.addIbis( theIbis );
        }
    }

    /**
     * An ibis has explicitly left the computation.
     * @param theIbis The ibis that left.
     */
    @Override
    public void left(IbisIdentifier theIbis) {
        for( Worker w: workers ){
            w.removeIbis( theIbis );
        }
        master.removeIbis( theIbis );
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
        super( "Node" );
	Properties ibisProperties = new Properties();
	ibisProperties.setProperty( "ibis.server.address", serverAddress );
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
        for( int i=0; i<numberOfProcessors; i++ ){
            Worker w = new Worker( ibis, master, i );
            w.start();
            master.waitForSubscription( w.identifier() );
            workers[i] = w;
        }
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

    @Override
    public void run()
    {
	try {
	    // FIXME: do something more appropriate.
            Thread.sleep( 10000 );
	    ibis.end();
	}
	catch( Exception x ) {
	    x.printStackTrace();
	}
    }

    /**
     * Gracefully shut down this node after all the jobs currently
     * in the queue have been processed.
     * This method returns after the node has been shut down.
     */
    public void finish() {
        
        for( Worker w: workers ){
            w.resignExcept( master.identifier() );
        }
	// We must close the master first before we even try to stop the
	// workers, since the master may need them to finish its jobs.
	master.setStopped();
        boolean masterActive = true;

        do {
            try {
                master.join();
            } catch (InterruptedException e) {
                // Not interesting.
            }
            masterActive = master.isAlive();
        } while( masterActive );

        // Now try to shut down all workers.
	
	// First let all workers close their input port and resign from all masters.
	for( Worker w: workers ) {
	    w.closeDown();
	}
	
	// Then wait for each worker to complete its work.
	// We can wait for all workers to finish by waiting
	// one by one for each worker to finish.
	for( Worker w: workers ) {
	    w.finish();
	}
	try {
	    ibis.end();
	}
	catch( IOException x ) {
	    // Nothing we can do about it.
	}
    }
}
