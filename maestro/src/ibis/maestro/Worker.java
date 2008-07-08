package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Job.JobIdentifier;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

/**
 * A worker in the Maestro workflow framework.
 * @author Kees van Reeuwijk
 */
@SuppressWarnings("synthetic-access")
public final class Worker extends Thread implements TaskSource, PacketReceiveListener<MasterMessage> {

    /** The list of all known masters, in the order that they were handed their
     * id. Thus, element <code>i</code> of the list is guaranteed to have
     * <code>i</code> as its id.
     */
    private final ArrayList<MasterInfo> masters = new ArrayList<MasterInfo>();

    /** The list of ibises we haven't registered with yet. */
    private final LinkedList<IbisIdentifier> unregisteredMasters = new LinkedList<IbisIdentifier>();

    /** The list of masters we should ask for extra work if we are bored. */
    private final ArrayList<MasterInfo> taskSources = new ArrayList<MasterInfo>();

    private final Node node;

    private final JobList jobs;

    private final PacketUpcallReceivePort<MasterMessage> receivePort;
    private final PacketSendPort<WorkerMessage> sendPort;
    private long queueEmptyMoment = 0L;
    private final WorkerQueue queue = new WorkerQueue();
    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    private final WorkThread workThreads[] = new WorkThread[numberOfProcessors];
    private boolean stopped = false;
    private final long startTime;
    private long activeTime = 0;
    private long stopTime = 0;
    private long idleDuration = 0;      // Cumulative idle time during the run.
    private int runningTasks = 0;
    private boolean askMastersForWork = true;
    private final Random rng = new Random();

    private HashMap<TaskType, WorkerTaskStats> taskStats = new HashMap<TaskType, WorkerTaskStats>();

    static final class MasterIdentifier implements Serializable {
	private static final long serialVersionUID = 7727840589973468928L;
	final int value;

	private MasterIdentifier( int value )
	{
	    this.value = value;
	}

	/**
	 * @return A hash code for this identifier.
	 */
	@Override
	public int hashCode() {
	    return value;
	}

	/**
	 * Compares this master identifier to the given object.
	 * @param obj The object to compare to.
	 * @return True iff the two identifiers are equal.
	 */
	@Override
	public boolean equals(Object obj) {
	    if (this == obj)
		return true;
	    if (obj == null)
		return false;
	    if (getClass() != obj.getClass())
		return false;
	    final MasterIdentifier other = (MasterIdentifier) obj;
	    if (value != other.value)
		return false;
	    return true;
	}


	/** Returns a string representation of this master.
	 * 
	 * @return The string representation.
	 */
	@Override
	public String toString()
	{
	    return "M" + value;
	}
    }

    /**
     * Creates a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to.
     * @param node The node this worker belongs to.
     * @param jobs The list of jobs.
     * @throws IOException Thrown if the construction of the worker failed.
     */
    Worker( Ibis ibis, Node node, JobList jobs ) throws IOException
    {
	super( "Worker" );   // Create a thread with a name.
	this.node = node;
	this.jobs = jobs;
	receivePort = new PacketUpcallReceivePort<MasterMessage>( ibis, Globals.workerReceivePortName, this );
	sendPort = new PacketSendPort<WorkerMessage>( ibis );
	for( int i=0; i<numberOfProcessors; i++ ) {
	    WorkThread t = new WorkThread( this, node );
	    workThreads[i] = t;
	    t.start();
	}
	long now = System.nanoTime();
	startTime = now;
	queueEmptyMoment = now;
    }

    /**
     * Start this worker thread.
     */
    @Override
    public void start()
    {
	receivePort.enable();           // We're open for business.
	super.start();                  // Start the thread
	registerWithMaster( node.ibisIdentifier() );
    }

    /**
     * Sets the local listener to the given class instance.
     * @param localListener The local listener to use.
     */
    void setLocalListener( PacketReceiveListener<WorkerMessage> localListener )
    {
	sendPort.setLocalListener( localListener );
    }

    /**
     * Sets the stopped state of this worker.
     */
    void setStopped()
    {
	synchronized( queue ) {
	    stopped = true;
	    queue.notifyAll();
	}
    }

    /**
     * Tells this worker not to ask for work any more.
     */
    void stopAskingForWork()
    {
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "Worker: don't ask for work" );
	}
	synchronized( queue ){
	    askMastersForWork = false;
	}
    }

    /**
     * Returns the identifier of the task subjob port of this worker.
     * @return The port identifier.
     */
    ReceivePortIdentifier identifier()
    {
	return receivePort.identifier();
    }

    /** Removes and returns a random task source from the list of
     * known task sources. Returns null if the list is empty.
     * 
     * @return The task source, or null if there isn't one.
     */
    private MasterInfo getRandomWorkSource()
    {
	MasterInfo res;

	synchronized( queue ){
	    if( !askMastersForWork ){
		return null;
	    }
	    while( true ){
		int size = taskSources.size();
		if( size == 0 ){
		    return null;
		}
		// There are masters on the explict task sources list,
		// draw a random one.
		int ix = rng.nextInt( size );
		res = taskSources.remove( ix );
		if( !res.isDead() ){
		    return res;
		}
		// The master we drew from the lottery is dead. Try again.
	    }
	}
    }

    /**
     * A new ibis has joined the computation.
     * @param theIbis The ibis that has joined.
     */
    void addUnregisteredMasters( IbisIdentifier theIbis, boolean local )
    {
	synchronized( queue ){
	    if( local ) {
		if( Settings.traceWorkerList ) {
		    Globals.log.reportProgress( "Local ibis " + theIbis + " need not be added to unregisteredMasters" );
		}
	    }
	    else {
		if( Settings.traceWorkerList ) {
		    Globals.log.reportProgress( "Non-local ibis " + theIbis + " must be added to unregisteredMasters" );
		}
		// FIXME: we don't need the notify for the local master.
		unregisteredMasters.addLast( theIbis );
	    }
	    if( activeTime == 0 || queueEmptyMoment != 0 ) {
		// We haven't done any work yet, or we are idle.
		queue.notifyAll();
	    }
	}
    }

    /**
     * Returns a random registered master.
     * @return The task source, or null if there isn't one.
     */
    private MasterInfo getRandomRegisteredMaster()
    {
	MasterInfo res;

	synchronized( queue ){
	    if( !askMastersForWork ){
		return null;
	    }
	    int size = masters.size();
	    if( size == 0 ) {
		// No masters at all, give up.
		return null;
	    }
	    int ix = rng.nextInt( size );
	    int n = size;
	    while( n>0 ) {
		// We have picked a random place in the list of known masters, don't
		// return a dead or unregistered one, so keep walking the list until we
		// encounter a good one.
		// We only try 'n' times, since the list may consist entirely of duds.
		// (And yes, these duds skew the probabilities, we don't care.)
		res = masters.get( ix );
		if( !res.isDead() && res.isRegistered() ) {
		    return res;
		}
		ix++;
		if( ix>=masters.size() ) {
		    // Wrap around.
		    ix = 0;
		}
		n--;
	    }
	    return null;
	}
    }

    /**
     * Returns the first element of the list of unregistered masters, or
     * <code>null</code> if there is nothing in the list.
     * @return An unregistered master.
     */
    private IbisIdentifier getUnregisteredMaster()
    {
	synchronized( queue ){
	    if( unregisteredMasters.isEmpty() ){
		return null;
	    }
	    return unregisteredMasters.removeFirst();
	}
    }

    private void registerWithMaster( IbisIdentifier ibis )
    {
	MasterIdentifier masterID;

	if( Settings.traceWorkerList ) {
	    Globals.log.reportProgress( "Worker " + node.ibisIdentifier() + ": sending registration message to ibis " + ibis );
	}
	synchronized( queue ){
	    // Reserve a slot for this master, and get an id.
	    masterID = new MasterIdentifier( masters.size() );
	    MasterInfo info = new MasterInfo( masterID, ibis );
	    masters.add( info );
	}
	TaskType taskTypes[] = jobs.getSupportedTaskTypes();
	RegisterWorkerMessage msg = new RegisterWorkerMessage( receivePort.identifier(), masterID, taskTypes );
	long sz = sendPort.tryToSend( ibis, Globals.masterReceivePortName, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
	if( sz<0 ) {
	    System.err.println( "Cannot register with master " + ibis );
	    node.declareIbisDead( ibis );
	}
    }

    private void sendUpdate( MasterInfo master )
    {
	CompletionInfo[] completionInfo = node.getCompletionInfo( jobs );
	WorkerQueueInfo[] workerQueueInfo = queue.getWorkerQueueInfo( taskStats );
	WorkerUpdateMessage msg = new WorkerUpdateMessage( master.getIdentifierOnMaster(), completionInfo, workerQueueInfo );

	long sz = sendPort.tryToSend( master.localIdentifier.value, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
	if( sz<0 ) {
	    master.declareDead();
	}
    }

    private void askMoreWork()
    {
	// Try to register with a new Ibis.
	IbisIdentifier newIbis = getUnregisteredMaster();
	if( newIbis != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: registering with master " + newIbis );
	    }
	    registerWithMaster( newIbis );
	    return;
	}

	// Try to tell a known master we want more tasks. We do this by
	// telling it about our current state.
	MasterInfo taskSource = getRandomWorkSource();
	if( taskSource != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: asking master " + taskSource.localIdentifier + " for work" );
	    }
	    sendUpdate( taskSource );
	    return;
	}

	// Finally, just tell a random master about our current work queues.
	taskSource = getRandomRegisteredMaster();
	if( taskSource != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: updating master " + taskSource.localIdentifier );
	    }
	    sendUpdate( taskSource );
	    return;
	}
    }

    private void handleWorkerAcceptMessage( WorkerAcceptMessage msg )
    {
	MasterInfo master;

	if( Settings.traceWorkerProgress ){
	    Globals.log.reportProgress( "Received a worker accept message " + msg );
	}
	synchronized( queue ){
	    sendPort.registerDestination( msg.port, msg.source.value );
	    master = masters.get( msg.source.value );
	    master.setIdentifierOnMaster( msg.identifierOnMaster );
	    queue.notifyAll();
	}
	sendUpdate( master );
    }

    /**
     * Handle a message containing a new task to run.
     * 
     * @param msg The message to handle.
     */
    private void handleRunTaskMessage( RunTaskMessage msg )
    {
	long now = System.nanoTime();
	MasterInfo mi;

	synchronized( queue ) {
	    if( activeTime == 0L ) {
		activeTime = now;
	    }
	    if( queueEmptyMoment>0L ){
		// The queue was empty before we entered this
		// task in it. Record this for the statistics.
		long queueEmptyInterval = now - queueEmptyMoment;
		idleDuration += queueEmptyInterval;
		queueEmptyMoment = 0L;
	    }
	    int length = queue.add( msg );
            msg.setQueueMoment( now, length );
	    mi = masters.get( msg.source.value );
	}
	if( mi != null ) {
	    sendUpdate( mi );
	}
	synchronized( queue ) {
	    queue.notifyAll();
	}
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
     * Handles task request message <code>msg</code>.
     * @param msg The task we received and will put in the queue.
     */
    public void messageReceived( MasterMessage msg )
    {
	if( Settings.traceWorkerProgress ){
	    Globals.log.reportProgress( "Worker: received message " + msg );
	}
	if( msg instanceof RunTaskMessage ){
	    RunTaskMessage runTaskMessage = (RunTaskMessage) msg;

	    handleRunTaskMessage( runTaskMessage );
	}
	else if( msg instanceof WorkerAcceptMessage ) {
	    WorkerAcceptMessage am = (WorkerAcceptMessage) msg;

	    handleWorkerAcceptMessage( am );
	}
	else {
	    Globals.log.reportInternalError( "FIXME: handle messages of type " + msg.getClass() );
	}
    }
    
    private WorkerTaskStats getWorkerTaskStats( TaskType type )
    {
        return taskStats.get( type );
    }

    /** Gets a task to execute.
     * @return The next task to execute.
     */
    @Override
    public RunTask getTask()
    {
	while( true ) {
	    boolean askForWork = false;
	    try {
		synchronized( queue ) {
		    if( queue.isEmpty() ) {
			if( queueEmptyMoment == 0 ) {
			    queueEmptyMoment = System.nanoTime();
			}
			if( stopped && runningTasks == 0 ) {
			    // No tasks in queue, and worker is stopped. Return null to
			    // indicate that there won't be further tasks.
			    break;
			}
			if( taskSources.isEmpty() && unregisteredMasters.isEmpty() ){
			    // There was no master to subscribe to, update, or ask for work.
			    if( Settings.traceWorkerProgress ) {
				System.out.println( "Worker: waiting for new tasks in queue" );
			    }
			    queue.wait();
			}
			else {
			    askForWork = true;
			}
		    }
		    else {
                        long now = System.nanoTime();
			runningTasks++;
			RunTaskMessage message = queue.remove();
                        TaskType type = message.task.type;
			message.setRunMoment( now );
                        long queueTime = now-message.getQueueMoment();
                        int queueLength = message.getQueueLength();
                        if( queueLength>0 ) {
                            WorkerTaskStats stats = getWorkerTaskStats( type );
                            stats.setQueueTimePerTask( queueTime/queueLength );
                        }
			Task task = findTask( message.task.type );
			if( Settings.traceWorkerProgress ) {
			    System.out.println( "Worker: handed out task " + message + " of type " + type + "; it was queued for " + Service.formatNanoseconds( queueTime ) + "; there are now " + runningTasks + " running tasks" );
			}
			return new RunTask( task, message );
		    }
		}
		if( askForWork ){
		    askMoreWork();
		}
	    }
	    catch( InterruptedException e ){
		// Not interesting.
	    }
	}
	return null;
    }


    /** Given a job identifier, return the index in <code>jobs</code>
     * of this identifier, or -1 if it doesn't exist.
     * @param job The job to search for.
     * @return The index of the job in <code>jobs</code>
     */
    private int searchJob( JobIdentifier job )
    {
	for( int i=0; i<jobs.size(); i++ ) {
	    Job t = jobs.get( i );
	    if( t.id.equals( job ) ){
		return i;
	    }
	}
	return -1;

    }

    /**
     * Given a task type, return the job it belongs to, or <code>null</code> if we
     * cannot find it. Since that is an internal error, report that error.
     * @param type
     * @return
     */
    private Job findJob( TaskType type )
    {
	int ix = searchJob( type.job );
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
    private Task findTask( TaskType type )
    {
	Job t = findJob( type );
	return t.tasks[type.taskNo];
    }

    /** Reports the result of the execution of a task. (Overrides method in superclass.)
     * @param task The task that was run.
     * @param result The result coming from the run task.
     */
    @Override
    public void reportTaskCompletion( RunTask task, Object result )
    {
	long now = System.nanoTime();
	long queueInterval = task.message.getRunMoment()-task.message.getQueueMoment();
	TaskType taskType = task.message.task.type;
	Job t = findJob( taskType );
	int nextTaskNo = taskType.taskNo+1;
        final MasterIdentifier master = task.message.source;

	CompletionInfo[] completionInfo = node.getCompletionInfo( jobs );
	WorkerQueueInfo[] workerQueueInfo = queue.getWorkerQueueInfo( taskStats );
	long workerDwellTime = System.nanoTime()-task.message.getQueueMoment();
	WorkerMessage msg = new TaskCompletedMessage( task.message.workerIdentifier, task.message.taskId, workerDwellTime, completionInfo, workerQueueInfo );
	long sz = sendPort.tryToSend( master.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );

	if( nextTaskNo<t.tasks.length ){
	    // There is a next step to take.
	    TaskInstance nextTask = new TaskInstance( task.message.task.jobInstance, t.getNextTaskType( taskType ), result );
	    node.submit( nextTask );
	}
	else {
	    // This was the final step. Report back the result.
	    JobInstanceIdentifier identifier = task.message.task.jobInstance;
	    sendResultMessage( identifier.receivePort, identifier, result );
	}

	// Update statistics and notify our own queue waiters that something
	// has happened.
	synchronized( queue ) {
	    final MasterInfo mi = masters.get( master.value );
	    if( mi != null ) {
		if( !Service.member( taskSources, mi ) ) {
		    taskSources.add( mi );
		}
	    }
	    if( !taskStats.containsKey( taskType ) ){
		taskStats.put( taskType, new WorkerTaskStats() );
	    }
	    WorkerTaskStats stats = getWorkerTaskStats( taskType );
	    stats.countTask( queueInterval, now-task.message.getRunMoment() );
	    runningTasks--;
	    if( Settings.traceRemainingJobTime ) {
		Globals.log.reportProgress( "Completed " + task.message.task + "; queueInterval=" + Service.formatNanoseconds( queueInterval ) + "; runningTasks=" + runningTasks );
	    }
	    queue.notifyAll();
	}
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "Completed task "  + task.message );
	}
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    void removeIbis( IbisIdentifier theIbis )
    {
	synchronized( queue ) {
	    for( MasterInfo master: masters ){
		if( master.ibis.equals( theIbis ) ){
		    // This ibis is now dead. Make it official.
		    master.declareDead();
		    break;   // There's supposed to be only one entry, so don't bother searching for more.
		}
	    }
	    // This is a good reason to wake up the queue.
	    queue.notifyAll();
	}
    }

    /** Runs this worker. */
    @Override
    public void run()
    {
	for( int i=0; i<numberOfProcessors; i++ ) {
	    Service.waitToTerminate( workThreads[i] );
	}
	stopTime = System.nanoTime();
    }

    /** Print some statistics about the entire worker run. */
    void printStatistics( PrintStream s )
    {
	jobs.printStatistics( s );
	if( stopTime<startTime ) {
	    System.err.println( "Worker didn't stop yet" );
	    stopTime = System.nanoTime();
	}
	if( activeTime<startTime ) {
	    System.err.println( "Worker was not used" );
	    activeTime = startTime;
	}
	long workInterval = stopTime-activeTime;
        queue.printStatistics( s );
	double idlePercentage = 100.0*((double) idleDuration/(double) workInterval);
	Set<TaskType> tl = taskStats.keySet();
	for( TaskType t: tl ){
	    WorkerTaskStats stats = getWorkerTaskStats( t );
	    stats.reportStats( s, t, workInterval );
	}
	s.printf(  "Worker: # threads       = %5d\n", workThreads.length );
	s.println( "Worker: run time        = " + Service.formatNanoseconds( workInterval ) );
	s.println( "Worker: activated after = " + Service.formatNanoseconds( activeTime-startTime ) );
	s.println( "Worker: total idle time = " + Service.formatNanoseconds( idleDuration ) + String.format( " (%.1f%%)", idlePercentage ) );
	sendPort.printStats( s, "worker send port" );
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * @param port The port to send the result to.
     * @param id The job instance identifier.
     * @param result The result to send.
     * @return The size of the sent message, or -1 if the transjob failed.
     */
    long sendResultMessage( ReceivePortIdentifier port, JobInstanceIdentifier id,
	    Object result ) {
	WorkerMessage msg = new JobResultMessage( id, result );
	return sendPort.tryToSend( port, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
    }
}
