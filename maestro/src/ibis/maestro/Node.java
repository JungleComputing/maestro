package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.Registry;
import ibis.ipl.RegistryEventHandler;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * A node in the Maestro dataflow network.
 * 
 * @author Kees van Reeuwijk
 *
 */
public final class Node extends Thread implements PacketReceiveListener<Message>
{
    final IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.MEMBERSHIP_UNRELIABLE, IbisCapabilities.ELECTIONS_STRICT );
    private final Ibis ibis;
    final PacketSendPort<Message> sendPort;
    final PacketUpcallReceivePort<Message> receivePort;
    final long startTime;
    long stopTime = 0;
    final Master master;
    private final Worker worker;
    private static final String MAESTRO_ELECTION_NAME = "maestro-election";
    RegistryEventHandler registryEventHandler;

    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    final ArrayList<WorkThread> workThreads = new ArrayList<WorkThread>();

    /** The estimated time it takes to send an administration message. */
    final TimeEstimate infoSendTime = new TimeEstimate( Service.MICROSECOND_IN_NANOSECONDS );

    /** The list of all known masters, in the order that they were handed their
     * id. Thus, element <code>i</code> of the list is guaranteed to have
     * <code>i</code> as its id.
     */
    final ArrayList<NodeInfo> masters = new ArrayList<NodeInfo>();

    /** The list of maestro nodes in this computation. */
    private final ArrayList<MaestroInfo> maestros = new ArrayList<MaestroInfo>();

    /** The list of running jobs with their completion listeners. */
    private final ArrayList<JobInstanceInfo> runningJobs = new ArrayList<JobInstanceInfo>();

    /** The list of now unsuspect ibises. */
    final IbisIdentifierList unsuspectNodes = new IbisIdentifierList();

    /** The list of nodes we want to accept. */
    final AcceptList acceptList = new AcceptList();

    /** The list of nodes we know about. */
    final WorkerList nodes = new WorkerList();

    private boolean isMaestro;
    final JobList jobs;

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
		addUnregisteredMaster( theIbis, local );
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
	    removeIbis( theIbis );
	    removeIbis( theIbis );
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
	    removeIbis( theIbis );
	    removeIbis( theIbis );
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

	this.jobs = jobs;
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
        for( int i=0; i<numberOfProcessors; i++ ) {
            WorkThread t = new WorkThread( this );
            workThreads.add( t );
            t.start();
        }
        receivePort = new PacketUpcallReceivePort<Message>( ibis, Globals.receivePortName, this );
        sendPort = new PacketSendPort<Message>( ibis, this );
        sendPort.setLocalListener( this );    // FIXME: no longer necessary
	master = new Master( ibis, this );
	worker = new Worker( this );
        startTime = System.nanoTime();
	start();
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
	Service.waitToTerminate( this );
	if( Settings.traceNodes ) {
	    Globals.log.reportProgress( "master is terminated; waiting for worker to terminate" );
	}
	
	// FIXME: do termination properly!!

	/** Once the master has stopped, stop the worker. */
	waitForWorkThreadsToTerminate();
	if( Settings.traceNodes ) {
	    Globals.log.reportProgress( "worker is terminated" );
	}
	printStatistics( System.out );
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
     * @return The ibis identifier of this node.
     */
    IbisIdentifier ibisIdentifier()
    {
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
        synchronized( master.queue ){
            nodes.setSuspect( theIbis );
        }
        synchronized( worker.queue ) {
            for( NodeInfo theMaster: masters ){
                if( theMaster.ibis.equals( theIbis ) ){
                    theMaster.setSuspect();
                    break;
                }
            }
        }       
    }

    /**
     * Set the given ibis as unsuspect on the master.
     * @param theIbis The now unsuspect ibis.
     */
    protected void setUnsuspectOnMaster( IbisIdentifier theIbis )
    {
        synchronized( master.queue ){
            nodes.setUnsuspect( theIbis );
        }
    }

    /**
     * Set the given ibis as unsuspect on the master.
     * @param theIbis The now unsuspect ibis.
     */
    protected void setUnsuspectOnWorker( IbisIdentifier theIbis )
    {
	setUnsuspect( theIbis );
    }
    
    void updateAdministration()
    {
        WorkerIdentifier workerToAccept = null;
        
        master.updateAdministration();
        workerToAccept = acceptList.removeIfAny();
        if( workerToAccept != null ) {
            boolean ok = sendAcceptMessage( workerToAccept );
            if( !ok ) {
                // Couldn't send an accept message: back on the list.
                acceptList.add( workerToAccept );
            }
        }
        worker.updateAdministration();
    }

    RunTask getTask()
    {
        return worker.getTask();
    }
    
    void reportTaskCompletion( RunTask task, Object result )
    {
        worker.reportTaskCompletion( task, result );
    }

    /**
     * Returns true iff this listener is associated with the given port.
     * @param port The port it should be associated with.
     * @return True iff this listener is associated with the port.
     */
    public boolean hasReceivePort( ReceivePortIdentifier port )
    {
        boolean res = port.equals( receivePort.identifier() );
        return res;
    }

    /**
     * Start this thread.
     */
    @Override
    public void start()
    {
        addUnregisteredMaster( ibisIdentifier(), true );
        receivePort.enable();           // We're open for business.
        super.start();                  // Start the thread
    }
    /**
     * Handles message <code>msg</code> from worker.
     * @param msg The message we received.
     */
    @Override
    public void messageReceived( Message msg, long arrivalMoment )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master: received message " + msg );
        }
        if( msg instanceof TaskCompletedMessage ) {
            TaskCompletedMessage result = (TaskCompletedMessage) msg;

            nodes.setUnsuspect( result.source, this );
            handleTaskCompletedMessage( result, arrivalMoment );
        }
        else if( msg instanceof JobResultMessage ) {
            JobResultMessage m = (JobResultMessage) msg;

            reportCompletion( m.job, m.result );
        }
        else if( msg instanceof WorkerUpdateMessage ) {
            WorkerUpdateMessage m = (WorkerUpdateMessage) msg;

            nodes.setUnsuspect( m.source, this );
            handleWorkerUpdateMessage( m, arrivalMoment );
        }
        else if( msg instanceof RegisterNodeMessage ) {
            RegisterNodeMessage m = (RegisterNodeMessage) msg;

            handleRegisterNodeMessage( m );
        }
        else if( msg instanceof NodeResignMessage ) {
            NodeResignMessage m = (NodeResignMessage) msg;

            unsubscribeWorker( m.source );
        }
        if( msg instanceof RunTaskMessage ){
            RunTaskMessage runTaskMessage = (RunTaskMessage) msg;

            handleRunTaskMessage( arrivalMoment, runTaskMessage );
        }
        else if( msg instanceof NodeAcceptMessage ) {
            NodeAcceptMessage am = (NodeAcceptMessage) msg;

            handleNodeAcceptMessage( am );
        }
        else {
            Globals.log.reportInternalError( "the master should handle message of type " + msg.getClass() );
        }
    }

    /** FIXME.
     * @param arrivalMoment
     * @param runTaskMessage
     */
    private void handleRunTaskMessage( long arrivalMoment, RunTaskMessage runTaskMessage )
    {
        worker.handleRunTaskMessage( runTaskMessage, arrivalMoment );
    }

    void printStatistics( java.io.PrintStream s )
    {
        if( stopTime<startTime ) {
            System.err.println( "Node didn't stop yet" );
            stopTime = System.nanoTime();
        }
        s.printf(  "# threads       = %5d\n", workThreads.size() );
        nodes.printStatistics( s );
        jobs.printStatistics( s );
        sendPort.printStatistics( s, "send port" );
        master.printStatistics( System.out );
        worker.printStatistics( System.out );
    }

    boolean registerWithNode( NodeInfo info )
    {
        boolean ok = true;
        if( Settings.traceWorkerList ) {
            Globals.log.reportProgress( "Node " + ibisIdentifier() + ": sending registration message to ibis " + info );
        }
        TaskType taskTypes[] = jobs.getSupportedTaskTypes();
        RegisterNodeMessage msg = new RegisterNodeMessage( receivePort.identifier(), taskTypes, info.localIdentifier );
        long sz = sendPort.tryToSend( info.ibis, Globals.receivePortName, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( sz<0 ) {
            System.err.println( "Cannot register with node " + info.ibis );
            setSuspect( info.ibis );
            ok = false;
        }
        return ok;
    }

    void sendUpdate( NodeInfo node )
    {
        CompletionInfo[] completionInfo = master.getCompletionInfo( jobs, nodes );
        WorkerQueueInfo[] workerQueueInfo = worker.queue.getWorkerQueueInfo( worker.taskStats );
        WorkerUpdateMessage msg = new WorkerUpdateMessage( node.getIdentifierOnMaster(), completionInfo, workerQueueInfo );
    
        // We ignore the result because we don't care about the message size,
        // and if the update failed, it failed.
        sendPort.tryToSend( node.localIdentifier.value, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
    }

    /**
     * A worker has sent us a message to register itself with us. This is
     * just to tell us that it's out there. We tell it what our receive port is,
     * and which handle we have assigned to it, so that it can then inform us
     * of the types of tasks it supports.
     * @param m The worker registration message.
     */
    void handleRegisterNodeMessage( RegisterNodeMessage m )
    {
        WorkerIdentifier workerID;
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master: received registration message " + m + " from worker " + m.port );
        }
        if( m.supportedTypes.length == 0 ) {
            Globals.log.reportInternalError( "Worker " + m.port + " has zero supported types??" );
        }
        boolean local = sendPort.isLocalListener( m.port );
        synchronized( master.queue ) {
            workerID = nodes.subscribeNode( receivePort.identifier(), m.port, local, m.masterIdentifier, m.supportedTypes );
        }
        sendPort.registerDestination( m.port, workerID.value );
        acceptList.add( workerID );
    }

    boolean sendAcceptMessage( WorkerIdentifier workerID )
    {
        ReceivePortIdentifier myport = receivePort.identifier();
        MasterIdentifier idOnWorker = nodes.getMasterIdentifier( workerID );
        NodeAcceptMessage msg = new NodeAcceptMessage( idOnWorker, myport, workerID );
        boolean ok = true;
    
        nodes.setPingStartMoment( workerID );
        long sz = sendPort.tryToSend( workerID.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( sz<0 ){
            ok = false;
        }
        return ok;
    }

    /**
     * A worker has sent use a completion message for a task. Process it.
     * @param result The status message.
     * @param arrivalMoment FIXME
     */
    void handleTaskCompletedMessage( TaskCompletedMessage result, long arrivalMoment )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received a worker task completed message " + result );
        }
        synchronized( master.queue ){
            nodes.registerTaskCompleted( result, arrivalMoment );
            master.countHandledTask();
        }
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * @param m The update message.
     * @param arrivalMoment The time in ns the message arrived.
     */
    void handleWorkerUpdateMessage( WorkerUpdateMessage m, long arrivalMoment )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received worker update message " + m );
        }
        synchronized( master.queue ){
            nodes.registerCompletionInfo( m.source, m.workerQueueInfo, m.completionInfo, arrivalMoment );
        }
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    void removeIbis( IbisIdentifier theIbis )
    {
        synchronized( master.queue ) {
            ArrayList<TaskInstance> orphans = nodes.removeWorker( theIbis );
            master.queue.add( orphans );
        }
        synchronized( worker.queue ) {
            for( NodeInfo theMaster: masters ){
                if( theMaster.ibis.equals( theIbis ) ){
                    // This ibis is now dead. Make it official.
                    theMaster.setDead();
                    break;   // There's supposed to be only one entry, so don't bother searching for more.
                }
            }
        }
    }

    void unsubscribeWorker( WorkerIdentifier theWorker )
    {
        synchronized( master.queue ){
            ArrayList<TaskInstance> orphans = nodes.removeWorker( theWorker );
            master.queue.add( orphans );
        }
    }

    void handleNodeAcceptMessage( NodeAcceptMessage msg )
    {
        NodeInfo theMaster;
        NodeInfo unsuspect = null;
    
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Received a node accept message " + msg );
        }
        sendPort.registerDestination( msg.port, msg.source.value );
        synchronized( worker.queue ){
            theMaster = masters.get( msg.source.value );
            theMaster.setIdentifierOnMaster( msg.identifierOnMaster );
            if( theMaster.isSuspect() && !theMaster.isDead() ) {
        	theMaster.setUnsuspect();
        	unsuspect = theMaster;
            }
            worker.queue.notifyAll();
        }
        if( unsuspect != null ) {
            setUnsuspectOnMaster( unsuspect.ibis );
        }
        sendUpdate( theMaster );
    }

    void waitForWorkThreadsToTerminate()
    {
        WorkThread t = null;
        while( true ){
            synchronized( workThreads ){
                if( t != null ){
                    workThreads.remove( t );  // Remove a terminated worker.
                }
                if( workThreads.isEmpty() ){
                    break;
                }
                t = workThreads.get( 0 );
            }
            Service.waitToTerminate( t );
        }
        stopTime = System.nanoTime();
    }

    void stopWorker( WorkThread thread )
    {
        thread.shutdown();
        // It may linger a bit, we don't care.
        workThreads.remove( thread );
    }

    WorkThread spawnExtraWorker()
    {
        WorkThread t = new WorkThread( this );
        workThreads.add( t );
        t.start();
        return t;
    }

    /**
     * Given a task type, return the job it belongs to, or <code>null</code> if we
     * cannot find it. Since that is an internal error, report that error.
     * @param type
     * @return
     */
    Job findJob( TaskType type )
    {
        // FIXME: move this method
        int ix = type.job.searchJob( worker );
        if( ix<0 ) {
            Globals.log.reportInternalError( "Unknown job id in task type " + type );
            return null;
        }
        return jobs.get( ix );
    }

    /**
     * Given a task type, return the task.
     * @param type The task type.
     * @return The task.
     */
    Task findTask( TaskType type )
    {
        Job t = findJob( type );
        return t.tasks[type.taskNo];
    }

    /**
     * A new ibis has joined the computation.
     * @param worker FIXME
     * @param theIbis The ibis that has joined.
     * @param local True iff this is the local master.
     */
    protected void addUnregisteredMaster( IbisIdentifier theIbis, boolean local )
    {
        NodeInfo info;
    
        synchronized( worker.queue ){
            // Reserve a slot for this master, and get an id.
            MasterIdentifier masterID = new MasterIdentifier( masters.size() );
            info = new NodeInfo( masterID, theIbis, local );
            masters.add( info );
        }
        worker.unregisteredNodes.add( info );
        if( local ) {
            if( Settings.traceWorkerList ) {
        	Globals.log.reportProgress( "Local ibis " + theIbis + " need not be added to unregisteredMasters" );
            }
        }
        else {
            if( Settings.traceWorkerList ) {
        	Globals.log.reportProgress( "Non-local ibis " + theIbis + " must be added to unregisteredMasters" );
            }
        }
    }

    /**
     * This ibis is no longer suspect.
     * @param theIbis The unusupected ibis.
     */
    void setUnsuspect( IbisIdentifier theIbis )
    {
        // FIXME: also drain this list somewhere!!!
        unsuspectNodes.add( theIbis );
    }


}
