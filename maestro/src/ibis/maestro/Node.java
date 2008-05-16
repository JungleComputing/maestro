package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.Registry;
import ibis.ipl.RegistryEventHandler;
import ibis.maestro.Task.TaskIdentifier;

import java.io.IOException;
import java.io.PrintStream;
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
    private ArrayList<Task> tasks = new ArrayList<Task>();
    private static int taskCounter = 0;

    /** The list of maestro nodes in this computation. */
    private final ArrayList<MaestroInfo> maestros = new ArrayList<MaestroInfo>();

    /** The list of running tasks with their completion listeners. */
    private final ArrayList<TaskInfo> runningTasks = new ArrayList<TaskInfo>();

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
	    worker.addJobSource( theIbis );
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

    private static class TaskInfo {
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
	private TaskInfo( final TaskInstanceIdentifier identifier, Task task, final CompletionListener listener )
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
     * @throws IbisCreationFailedException Thrown if for some reason we cannot create an ibis.
     * @throws IOException Thrown if for some reason we cannot communicate.
     */
    public Node() throws IbisCreationFailedException, IOException
    {
	this( true );
    }

    /**
     * Constructs a new Maestro node using the given name server and completion listener.
     * @param runForMaestro If true, try to get elected as maestro.
     * @throws IbisCreationFailedException Thrown if for some reason we cannot create an ibis.
     * @throws IOException Thrown if for some reason we cannot communicate.
     */
    @SuppressWarnings("synthetic-access")
    public Node( boolean runForMaestro ) throws IbisCreationFailedException, IOException
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
	worker = new Worker( ibis, this );
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

    /** Print extensive statistics of this node.
     * @param s The stream to print to.
     */
    public void printStatistics( PrintStream s )
    {
	for( Task t: tasks ) {
	    t.printStatistics( s );
	}
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
	printStatistics( System.out );
	master.printStatistics();
	worker.printStatistics();
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

    /**
     * Given a job type, records the fact that it can be executed by
     * the worker of this node.
     * @param jobType The allowed job type.
     */
    public void allowJobType( JobType jobType )
    {
	worker.allowJobType( jobType );
    }

    /** Report the completion of the task with the given identifier.
     * @param id The task that has been completed.
     * @param result The task result.
     */
    @SuppressWarnings("synthetic-access")
    void reportCompletion( TaskInstanceIdentifier id, Object result )
    {
	TaskInfo task = null;

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
	    task.listener.jobCompleted( this, id.userId, result );
	}
    }

    @SuppressWarnings("synthetic-access")
    void addRunningTask( TaskInstanceIdentifier id, Task task, CompletionListener listener )
    {
	synchronized( runningTasks ){
	    runningTasks.add( new TaskInfo( id, task, listener ) );
	}
    }

    void submit( JobInstance j )
    {
	master.submit( j );
    }

    /** Start an extra work thread to replace the one that is blocked.
     * @return The newly started work thread.
     */
    WorkThread startExtraWorker()
    {
	WorkThread t = new WorkThread( worker, this );
	t.start();
	return t;
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

    /**
     * Given a job type, return the estimated average time it will take
     * to execute all subsequent jobs in the task by
     * the fastest route.
     * 
     * @param jobType The type of the job.
     * @return The estimated time in nanoseconds.
     */
    long getRemainingJobsTime( JobType jobType )
    {
	int ix = searchTask( jobType.task );
	if( ix<0 ) {
	    Globals.log.reportInternalError( "Unknown task id in job type " + jobType );
	    return 0L;
	}
	Task t = tasks.get(ix);
	JobType nextJobType = t.getNextJobType( jobType );
	if( nextJobType == null ) {
	    // There is no next job type; that's an easy estimate to make.
	    return 0L;
	}
	return master.getAverageCompletionTime( nextJobType );
    }

    /**
     * Creates a task with the given name and the given sequence of jobs.
     * 
     * @param name The name of the task.
     * @param jobs The list of jobs of the task.
     * @return A new task instance representing this task.
     */
    public Task createTask( String name, Job... jobs )
    {
	int taskId = taskCounter++;
	Task task = new Task( this, taskId, name, jobs );

	tasks.add( task );
	worker.registerTask( task );
	return task;
    }

    ReceivePortIdentifier identifier()
    {
	return master.identifier();
    }

    /** Given a task identifier, return the index in <code>tasks</code>
     * of this identifier, or -1 if it doesn't exist.
     * @param task The task to search for.
     * @return The index of the task in <code>tasks</code>
     */
    private int searchTask( TaskIdentifier task )
    {
	for( int i=0; i<tasks.size(); i++ ) {
	    Task t = tasks.get( i );
	    if( t.id.equals( task ) ){
		return i;
	    }
	}
	return -1;

    }

    /** Runs the given job instance.
     * 
     * @param job The job instance to run.
     */
    public void run( JobInstance job )
    {
	JobType type = job.type;
	TaskIdentifier task = type.task;

	int ix = searchTask( task );
	if( ix<0 ) {
	    Globals.log.reportInternalError( "Unknown task id " + task );
	    return;
	}
	Task t = tasks.get( ix );
	Job j = t.jobs[type.jobNo];

	Object result = j.run( job.input, this );
	int nextJobNo = type.jobNo+1;
	if( nextJobNo<t.jobs.length ){
	    // There is a next step to take.
	    JobInstance nextJob = new JobInstance( job.taskInstance, new JobType( type.task, nextJobNo ), result );
	    submit( nextJob );
	}
	else {
	    // This was the final step. Report back the result.
	    TaskInstanceIdentifier identifier = job.taskInstance;
	    sendResultMessage( identifier.receivePort, identifier, result );
	}
    }
}