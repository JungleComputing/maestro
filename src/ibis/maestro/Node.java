package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.Registry;
import ibis.ipl.RegistryEventHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * A node in the Maestro dataflow network.
 * 
 * @author Kees van Reeuwijk
 *
 */
public final class Node {
    final IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.MEMBERSHIP_UNRELIABLE, IbisCapabilities.ELECTIONS_STRICT );
    private final Ibis ibis;
    private final Master master;
    private final Worker worker;
    private static final String MAESTRO_ELECTION_NAME = "maestro-election";
    RegistryEventHandler registryEventHandler;

    /** The list of maestro nodes in this computation. */
    private ArrayList<MaestroInfo> maestros = new ArrayList<MaestroInfo>();
    private boolean isMaestro;

    private class NodeRegistryEventHandler implements RegistryEventHandler {

	/**
	 * A new Ibis joined the computation.
	 * @param theIbis The ibis that joined the computation.
	 */
	@Override
	public void joined( IbisIdentifier theIbis )
	{
	    registerIbisJoined( theIbis );
	    worker.addJobSource( theIbis );
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
	 * @param name The name of the election.
	 * @param theIbis The ibis that was elected.
	 */
	@Override
	public void electionResult( String name, IbisIdentifier theIbis )
	{
	    if( name.equals( MAESTRO_ELECTION_NAME ) && theIbis != null ){
	        synchronized( maestros ){
	            maestros.add( new MaestroInfo( theIbis ) );
	        }
	    }
	}

	/**
	 * Our ibis got a signal.
	 * @param signal The signal.
	 */
	@Override
	public void gotSignal( String signal )
	{
	    // Not interested.
	}
	
    }

    /** The list of maestros in this computation. */
    private static class MaestroInfo {
	IbisIdentifier ibis;   // The identifier of the maestro.

	MaestroInfo( IbisIdentifier id ) {
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
        synchronized( maestros ){
            for( MaestroInfo m: maestros ) {
                if( m.ibis.equals( id ) ) {
                    System.out.println( "Maestro ibis " + id + " was registered" );
                }
            }
        }
    }

    /** Registers the ibis with the given identifier as one that has left the
     * computation.
     * @param id The ibis that has left.
     */
    private void registerIbisLeft( IbisIdentifier id )
    {
        boolean noMaestrosLeft = false;

        synchronized( maestros ){
            int ix = maestros.size();

            while( ix>0 ) {
                ix--;
                MaestroInfo m = maestros.get( ix );
                if( m.ibis.equals( id ) ) {
                    maestros.remove( ix );
                    noMaestrosLeft = maestros.isEmpty();
                }
            }
        }
        if( noMaestrosLeft ) {
            System.out.println( "No maestros left; stopping.." );
            setStopped();
        }
    }

    /**
     * Constructs a new Maestro node using the given name server and completion listener.
     * @param listener A completion listener for computations completed by this node.
     * @param typeAdder The type deduction class.
     * @throws IbisCreationFailedException Thrown if for some reason we cannot create an ibis.
     * @throws IOException Thrown if for some reason we cannot communicate.
     */
    public Node( CompletionListener listener, TypeAdder typeAdder ) throws IbisCreationFailedException, IOException
    {
        this( listener, typeAdder, true );
    }

    /**
     * Constructs a new Maestro node using the given name server and completion listener.
     * @param listener A completion listener for computations completed by this node.
     * @param typeAdder The list of types this job allows.
     * @param runForMaestro If true, try to get elected as maestro.
     * @throws IbisCreationFailedException Thrown if for some reason we cannot create an ibis.
     * @throws IOException Thrown if for some reason we cannot communicate.
     */
    public Node(CompletionListener listener, TypeAdder typeAdder, boolean runForMaestro) throws IbisCreationFailedException, IOException
    {
        Properties ibisProperties = new Properties();
        IbisIdentifier maestro;

        ibisProperties.setProperty( "ibis.pool.name", "MaestroPool" );
        registryEventHandler = new NodeRegistryEventHandler();
        ibis = IbisFactory.createIbis(
                ibisCapabilities,
                ibisProperties,
                true,
                registryEventHandler,
                PacketSendPort.portType,
                PacketUpcallReceivePort.portType,
                PacketBlockingReceivePort.portType
        );
        if( Settings.traceNodes ) {
	    System.out.println( "Created ibis " + ibis );
	}
        Registry registry = ibis.registry();
        if( runForMaestro ){
            maestro = registry.elect( MAESTRO_ELECTION_NAME );
            isMaestro = maestro.equals( ibis.identifier() );
        }
        else {
            isMaestro = false;
            
        }
        if( Settings.traceNodes ) {
	    System.out.println( "Ibis " + ibis.identifier() + ": isMaestro=" + isMaestro );
	}
        master = new Master( ibis, this, listener );
        worker = new Worker( ibis, this, typeAdder );
        master.setLocalListener( worker );
        worker.setLocalListener( master );
        master.start();
        worker.start();
        registry.enableEvents();
        if( Settings.traceNodes ) {
            Globals.log.log( "Started a Maestro node" );
        }
    }

    void updateNeighborJobTypes( JobType jobTypes[] )
    {
	worker.updateNeighborJobTypes( jobTypes );
    }

    /** Submits the given job to this node.
     * @param j The job to submit.
     */
    public void submit( Job j )
    {
	master.submitExternalJob( j );
    }
    
    /** Set this node to the stopped state.
     * This does not mean that the node stops immediately,
     * but it does mean the master and worker try to wind down the work.
     */
    public void setStopped()
    {
        worker.stopAskingForWork();
        master.setStopped();
    }

    /**
     * Wait for this node to finish.
     */
    public void waitToTerminate()
    {
        /**
         * Everything interesting happens in the master and worker.
         * So all we do here is wait for the master and worker to terminate.
         * We only stop this thread if both are terminated, so we can just wait
         * for one to terminate, and then the other.
         */
        Service.waitToTerminate( master );

        /** Once the master has stopped, stop the worker. */
        worker.setStopped();
        Service.waitToTerminate( worker );
        master.printStatistics();
        worker.printStatistics();
        try {
            ibis.end();
        }
        catch( IOException x ) {
            // Nothing we can do about it.
        }
        if( Settings.traceNodes ) {
        System.out.println( "Node has terminated" );
	}
    }

    /**
     * Given a job type, records the fact that it can be executed by
     * the worker of this node.
     * @param jobType The allowed job type.
     */
    public void allowJobType( JobType jobType )
    {
	worker.allowJobType( jobType );
    }

    /** FIXME.
     * @param id
     * @param result
     */
    public void reportCompletion( TaskIdentifier id, JobResultValue result )
    {
        master.reportCompletion( id, result );        
    }

    public void submitTask(Job j, CompletionListener waiter, TaskIdentifier id) {
	// TODO: Auto-generated method stub
	
    }
}
