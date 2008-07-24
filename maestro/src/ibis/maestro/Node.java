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

    /** The list of running jobs with their completion listeners. */
    private final ArrayList<JobInstanceInfo> runningJobs = new ArrayList<JobInstanceInfo>();

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
	    if( !local ) {
		worker.addUnregisteredMaster( theIbis, local );
	    }
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

    private static class JobInstanceInfo {
	final JobInstanceIdentifier identifier;
	final Job job;
	final CompletionListener listener;
	private final long startTime = System.nanoTime();

	/**
	 * Constructs an information class for the given job identifier.
	 * 
	 * @param identifier The job identifier.
	 * @param job The job this belongs to.
	 * @param listener The completion listener associated with the job.
	 */
	private JobInstanceInfo( final JobInstanceIdentifier identifier, Job job, final CompletionListener listener )
	{
	    this.identifier = identifier;
	    this.job = job;
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
     * @param jobs The jobs that should be supported in this node.
     * @param runForMaestro If true, try to get elected as maestro.
     * @throws IbisCreationFailedException Thrown if for some reason we cannot create an ibis.
     * @throws IOException Thrown if for some reason we cannot communicate.
     */
    @SuppressWarnings("synthetic-access")
    public Node( JobList jobs, boolean runForMaestro ) throws IbisCreationFailedException, IOException
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
	worker = new Worker( ibis, this, jobs );
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
	if( Settings.traceNodes ) {
	    Globals.log.reportProgress( "Set node to stopped state. Telling worker..." );
	}
	worker.stopAskingForWork();
	if( Settings.traceNodes ) {
	    Globals.log.reportProgress( "Told worker. Telling master..." );
	}
	master.setStopped();
	if( Settings.traceNodes ) {
	    Globals.log.reportProgress( "Told master." );
	}
    }

    /**
     * Wait for this node to finish.
     */
    public void waitToTerminate()
    {
	if( Settings.traceNodes ) {
	    Globals.log.reportProgress( "Waiting for master to terminate" );
	}
	/**
	 * Everything interesting happens in the master and worker.
	 * So all we do here is wait for the master and worker to terminate.
	 * We only stop this thread if both are terminated, so we can just wait
	 * for one to terminate, and then the other.
	 */
	Service.waitToTerminate( master );
	if( Settings.traceNodes ) {
	    Globals.log.reportProgress( "master is terminated; waiting for worker to terminate" );
	}

	/** Once the master has stopped, stop the worker. */
	worker.setStopped();
	Service.waitToTerminate( worker );
	if( Settings.traceNodes ) {
	    Globals.log.reportProgress( "worker is terminated" );
	}
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

    /** Report the completion of the job with the given identifier.
     * @param id The job that has been completed.
     * @param result The job result.
     */
    @SuppressWarnings("synthetic-access")
    void reportCompletion( JobInstanceIdentifier id, Object result )
    {
	JobInstanceInfo job = null;

	synchronized( runningJobs ){
	    for( int i=0; i<runningJobs.size(); i++ ){
		job = runningJobs.get( i );
		if( job.identifier.equals( id ) ){
		    long jobInterval = System.nanoTime()-job.startTime;
		    job.job.registerJobTime( jobInterval );
		    runningJobs.remove( i );
		    break;
		}
	    }
	}
	if( job != null ){
	    job.listener.jobCompleted( this, id.userId, result );
	}
    }

    @SuppressWarnings("synthetic-access")
    void addRunningJob( JobInstanceIdentifier id, Job job, CompletionListener listener )
    {
	synchronized( runningJobs ){
	    runningJobs.add( new JobInstanceInfo( id, job, listener ) );
	}
    }

    void submit( TaskInstance j )
    {
	master.submit( j );
    }

    /**
     * 
     * @param receivePort The port to send it to.
     * @param id The identifier of the job.
     * @param result The result.
     * @return The size of the transmitted message, or -1 if the transjob failed.
     */
    long sendResultMessage( ReceivePortIdentifier receivePort, JobInstanceIdentifier id,
	    Object result ) {
	return worker.sendResultMessage( receivePort, id, result );
    }

    ReceivePortIdentifier identifier()
    {
	return master.identifier();
    }

    CompletionInfo[] getCompletionInfo( JobList jobs )
    {
	return master.getCompletionInfo( jobs );
    }

    /**
     * @return The ibis identifier of this node.
     */
    IbisIdentifier ibisIdentifier() {
	return ibis.identifier();
    }

    /** This ibis was reported as 'may be dead'. Try
     * not to communicate with it.
     * @param theIbis The ibis that may be dead.
     */
    protected void setSuspect( IbisIdentifier theIbis )
    {
	try {
	    ibis.registry().assumeDead( theIbis );
	}
	catch( IOException e )
	{
	    // Nothing we can do about it.
	}
	master.setSuspect( theIbis );
	worker.setSuspect( theIbis );
    }

    /**
     * Set the given ibis as unsuspect on the master.
     * @param theIbis The now unsuspect ibis.
     */
    protected void setUnsuspectOnMaster( IbisIdentifier theIbis )
    {
	master.setUnsuspect( theIbis );
    }

    /**
     * Set the given ibis as unsuspect on the master.
     * @param theIbis The now unsuspect ibis.
     */
    protected void setUnsuspectOnWorker( IbisIdentifier theIbis )
    {
	worker.setUnsuspect( theIbis );
    }

    WorkThread spawnExtraWorker()
    {
	return worker.spawnExtraWorker();
    }

    void stopWorker( WorkThread thread )
    {
	worker.stopWorker( thread );
    }
}
