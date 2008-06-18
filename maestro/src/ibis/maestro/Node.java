package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
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
    private final ArrayList<MaestroInfo> maestros = new ArrayList<MaestroInfo>();

    /** The list of running tasks with their completion listeners. */
    private final ArrayList<TaskInstanceInfo> runningTasks = new ArrayList<TaskInstanceInfo>();

    private boolean isMaestro;

    private class NodeRegistryEventHandler implements RegistryEventHandler {

	/**
	 * A new Ibis joined the computation.
	 * @param theIbis The ibis that joined the computation.
	 */
	@SuppressWarnings("synthetic-access")
	@Override
	public void joined( IbisIdentifier theIbis )
	{
	    registerIbisJoined( theIbis );
            boolean local = theIbis.equals( ibis.identifier() );
	    worker.addUnregisteredMasters( theIbis, local );
	}

	/**
	 * An ibis has died.
	 * @param theIbis The ibis that died.
	 */
	@SuppressWarnings("synthetic-access")
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
	@SuppressWarnings("synthetic-access")
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
	@SuppressWarnings("synthetic-access")
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

    private static class TaskInstanceInfo {
	final TaskInstanceIdentifier identifier;
	final Task task;
	final CompletionListener listener;
	private final long startTime = System.nanoTime();

	/**
	 * Constructs an information class for the given task identifier.
	 * 
	 * @param identifier The task identifier.
	 * @param task The task this belongs to.
	 * @param listener The completion listener associated with the task.
	 */
	private TaskInstanceInfo( final TaskInstanceIdentifier identifier, Task task, final CompletionListener listener )
	{
	    this.identifier = identifier;
	    this.task = task;
	    this.listener = listener;
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
		    Globals.log.reportProgress( "Maestro ibis " + id + " was registered" );
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
	    Globals.log.reportProgress( "No maestros left; stopping.." );
	    setStopped();
	}
    }

    /**
     * Constructs a new Maestro node using the given name server and completion listener.
     * @param runForMaestro If true, try to get elected as maestro.
     * @throws IbisCreationFailedException Thrown if for some reason we cannot create an ibis.
     * @throws IOException Thrown if for some reason we cannot communicate.
     */
    @SuppressWarnings("synthetic-access")
    public Node( TaskList tasks, boolean runForMaestro ) throws IbisCreationFailedException, IOException
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
	    Globals.log.reportProgress( "Created ibis " + ibis );
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
	    Globals.log.reportProgress( "Ibis " + ibis.identifier() + ": isMaestro=" + isMaestro );
	}
	master = new Master( ibis, this );
	worker = new Worker( ibis, this, tasks );
	master.setLocalListener( worker );
	worker.setLocalListener( master );
	master.start();
	worker.start();
	registry.enableEvents();
	if( Settings.traceNodes ) {
	    Globals.log.log( "Started a Maestro node" );
	}
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
	master.printStatistics( System.out );
	worker.printStatistics( System.out );
	try {
	    ibis.end();
	}
	catch( IOException x ) {
	    // Nothing we can do about it.
	}
	if( Settings.traceNodes ) {
	    Globals.log.reportProgress( "Node has terminated" );
	}
    }

    /** Report the completion of the task with the given identifier.
     * @param id The task that has been completed.
     * @param result The task result.
     */
    @SuppressWarnings("synthetic-access")
    void reportCompletion( TaskInstanceIdentifier id, Object result )
    {
	TaskInstanceInfo task = null;

	synchronized( runningTasks ){
	    for( int i=0; i<runningTasks.size(); i++ ){
		task = runningTasks.get( i );
		if( task.identifier.equals( id ) ){
		    long taskInterval = System.nanoTime()-task.startTime;
		    task.task.registerTaskTime( taskInterval );
		    runningTasks.remove( i );
		    break;
		}
	    }
	}
	if( task != null ){
	    task.listener.taskCompleted( this, id.userId, result );
	}
    }

    @SuppressWarnings("synthetic-access")
    void addRunningTask( TaskInstanceIdentifier id, Task task, CompletionListener listener )
    {
	synchronized( runningTasks ){
	    runningTasks.add( new TaskInstanceInfo( id, task, listener ) );
	}
    }

    void submit( JobInstance j )
    {
	master.submit( j );
    }
    
    long submitAndGetInfo( JobInstance j )
    {
	return master.submitAndGetInfo( j );
    }

    /**
     * 
     * @param receivePort The port to send it to.
     * @param id The identifier of the task.
     * @param result The result.
     * @return The size of the transmitted message, or -1 if the transmission failed.
     */
    long sendResultMessage( ReceivePortIdentifier receivePort, TaskInstanceIdentifier id,
	    Object result ) {
	return worker.sendResultMessage( receivePort, id, result );
    }

    /** Try to tell the cooperating ibises that this one is
     * dead.
     * @param theIbis The Ibis we think is dead.
     */
    public void declareIbisDead(IbisIdentifier theIbis)
    {
	try {
	    ibis.registry().assumeDead( theIbis );
	}
	catch( IOException e )
	{
	    // Nothing we can do about it.
	}
    }

    ReceivePortIdentifier identifier()
    {
	return master.identifier();
    }

    CompletionInfo[] getCompletionInfo( TaskList tasks )
    {
	return master.getCompletionInfo( tasks );
    }

    /**
     * @return
     */
    IbisIdentifier ibisIdentifier() {
        return ibis.identifier();
    }
}
