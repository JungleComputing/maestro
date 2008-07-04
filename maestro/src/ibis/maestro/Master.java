package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;

/**
 * A master in the Maestro flow graph framework.
 * 
 * @author Kees van Reeuwijk
 * 
 */
@SuppressWarnings("synthetic-access")
public class Master extends Thread implements PacketReceiveListener<WorkerMessage>
{
    private final Node node;
    private final WorkerList workers = new WorkerList();
    private final PacketUpcallReceivePort<WorkerMessage> receivePort;
    private final PacketSendPort<MasterMessage> sendPort;
    private final MasterQueue queue;

    private boolean stopped = false;
    private long nextTaskId = 0;
    private long incomingTaskCount = 0;
    private long handledTaskCount = 0;
    private final long startTime;
    private long stopTime = 0;

    /**
     * A worker identifier.
     * This is in essence just an int, but we encapsulate it to make
     * sure we don't mix it up with other kinds of identifier that
     * we use.
     * @author Kees van Reeuwijk
     *
     */
    static final class WorkerIdentifier implements Serializable {
	private static final long serialVersionUID = 3271311796768467853L;
	final int value;

	WorkerIdentifier( int value )
	{
	    this.value = value;
	}

	/**
	 * Returns the hash code of this worker identifier.
	 * @return A hash code for this identifier.
	 */
	@Override
	public int hashCode() {
	    return value;
	}

	/**
	 * Returns true iff this worker identifier is equal to the given
	 * one.
	 * @param obj The object to compare to.
	 * @return True iff the two identifiers are equal.
	 */
	@Override
	public boolean equals( Object obj )
	{
	    if (this == obj)
		return true;
	    if (obj == null)
		return false;
	    if (getClass() != obj.getClass())
		return false;
	    final WorkerIdentifier other = (WorkerIdentifier) obj;
	    if (value != other.value)
		return false;
	    return true;
	}

	/** Returns a string representation of this worker.
	 * 
	 * @return The string representation.
	 */
	@Override
	public String toString()
	{
	    return "W" + value;
	}
    }

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
     * @param node The node this master belongs to.
     * @throws IOException Thrown if the master cannot be created.
     */
    Master( Ibis ibis, Node node ) throws IOException
    {
	super( "Master" );
	this.queue = new MasterQueue();
	this.node = node;
	sendPort = new PacketSendPort<MasterMessage>( ibis );
	receivePort = new PacketUpcallReceivePort<WorkerMessage>( ibis, Globals.masterReceivePortName, this );
	startTime = System.nanoTime();
    }

    /**
     * Start this master thread.
     */
    @Override
    public void start()
    {
	receivePort.enable();           // We're open for business.
	super.start();                  // Start the thread
    }

    /**
     * Set the local listener to the given class instance.
     * @param localListener The local listener to use.
     */
    void setLocalListener( PacketReceiveListener<MasterMessage> localListener )
    {
	sendPort.setLocalListener( localListener );
    }

    void setStopped()
    {
	if( Settings.traceMasterProgress ){
	    Globals.log.reportProgress( "Master is set to stopped state" );
	}
	synchronized( queue ) {
	    stopped = true;
	    queue.notifyAll();
	}
    }

    /**
     * Returns true iff this master is in stopped mode, has no
     * tasks in its queue, and has not outstanding tasks on its workers.
     * @return True iff this master has processed all tasks it ever will.
     */
    private boolean isFinished()
    {
	synchronized( queue ){
	    if( !stopped ){
		return false;
	    }
	    if( !queue.isEmpty() ) {
		return false;
	    }
	    return workers.areIdle();
	}
    }

    private void unsubscribeWorker( WorkerIdentifier worker )
    {
	if( Settings.traceWorkerList ) {
	    System.out.println( "unsubscribe of worker " + worker );
	}
	synchronized( queue ){
	    workers.removeWorker( worker );
	    queue.notifyAll();
	}
    }

    /**
     * A worker has sent use a status message for a task. Process it.
     * @param result The status message.
     */
    private void handleTaskCompletedMessage( TaskCompletedMessage result )
    {
	if( Settings.traceMasterProgress ){
	    Globals.log.reportProgress( "Received a worker task completed message " + result );
	}
	synchronized( queue ){
	    workers.registerTaskCompleted( result );
	    handledTaskCount++;
	}
        submitAllPossibleTasks();
    }

    private void handleJobResultMessage( JobResultMessage m )
    {
	node.reportCompletion( m.job, m.result );
    }

    private void sendAcceptMessage( WorkerIdentifier workerID )
    {
	ReceivePortIdentifier myport = receivePort.identifier();
	Worker.MasterIdentifier idOnWorker = workers.getMasterIdentifier( workerID );
	WorkerAcceptMessage msg = new WorkerAcceptMessage( idOnWorker, myport, workerID );

	workers.registerPingTime( workerID );
	long sz = sendPort.tryToSend( workerID.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
	if( sz<0 ){
	    synchronized( queue ) {
		workers.declareDead( workerID );
		queue.notifyAll();
	    }
	}
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * 
     * @param m The update message.
     */
    private void handleWorkerUpdateMessage( WorkerUpdateMessage m )
    {
	if( Settings.traceMasterProgress ){
	    Globals.log.reportProgress( "Received worker update message " + m );
	}
	synchronized( queue ){
	    workers.registerCompletionInfo( m.source, m.workerQueueInfo, m.completionInfo );
	    queue.notifyAll();
	}
    }

    /**
     * A worker has sent us a message to register itself with us. This is
     * just to tell us that it's out there. We tell it what our receive port is,
     * and which handle we have assigned to it, so that it can then inform us
     * of the types of tasks it supports.
     *
     * @param m The worker registration message.
     */
    private void handleRegisterWorkerMessage( RegisterWorkerMessage m )
    {
	WorkerIdentifier workerID;
	ReceivePortIdentifier worker = m.port;

	if( Settings.traceMasterProgress ){
	    Globals.log.reportProgress( "Master: received registration message " + m + " from worker " + worker );
	}
	if( m.supportedTypes.length == 0 ) {
	    Globals.log.reportInternalError( "Worker " + worker + " has zero supported types??" );
	}
	synchronized( queue ) {
	    boolean local = sendPort.isLocalListener( m.port );
	    workerID = workers.subscribeWorker( receivePort.identifier(), worker, local, m.masterIdentifier, m.supportedTypes );
	    sendPort.registerDestination( worker, workerID.value );
	}
	sendAcceptMessage( workerID );
	synchronized( queue ) {
	    queue.notifyAll();
	}
    }

    /**
     * Returns true iff this listener is associated with the given port.
     * @param port The port it should be associated with.
     * @return True iff this listener is associated with the port.
     */
    @Override
    public boolean hasReceivePort( ReceivePortIdentifier port )
    {
	boolean res = port.equals( receivePort.identifier() );
	return res;
    }

    /**
     * Handles message <code>msg</code> from worker.
     * @param msg The message we received.
     */
    @Override
    public void messageReceived( WorkerMessage msg )
    {
	if( Settings.traceMasterProgress ){
	    Globals.log.reportProgress( "Master: received message " + msg );
	}
	if( msg instanceof TaskCompletedMessage ) {
	    TaskCompletedMessage result = (TaskCompletedMessage) msg;

	    handleTaskCompletedMessage( result );
	}
	else if( msg instanceof JobResultMessage ) {
	    JobResultMessage m = (JobResultMessage) msg;

	    handleJobResultMessage( m );
	}
	else if( msg instanceof WorkerUpdateMessage ) {
	    WorkerUpdateMessage m = (WorkerUpdateMessage) msg;

	    handleWorkerUpdateMessage( m );
	}
	else if( msg instanceof RegisterWorkerMessage ) {
	    RegisterWorkerMessage m = (RegisterWorkerMessage) msg;

	    handleRegisterWorkerMessage( m );
	}
	else if( msg instanceof WorkerResignMessage ) {
	    WorkerResignMessage m = (WorkerResignMessage) msg;

	    unsubscribeWorker( m.source );
	}
	else {
	    Globals.log.reportInternalError( "the master should handle message of type " + msg.getClass() );
	}
    }

    /**
     * @param worker The worker to send the task to.
     * @param task The task to send.
     */
    private void submitTaskToWorker( Subjob sub )
    {
	long taskId;

	synchronized( queue ){
	    taskId = nextTaskId++;
	    sub.worker.registerTaskStart( sub.task, taskId );
	}
	RunTaskMessage msg = new RunTaskMessage( sub.worker.identifierWithWorker, sub.worker.identifier, sub.task, taskId );
	long sz = sendPort.tryToSend( sub.worker.identifier.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
	if( sz<0 ){
	    // Try to put the paste back in the tube.
	    synchronized( queue ){
		sub.worker.retractTask( msg.taskId );
		queue.submit( msg.task );
	    }
	}
    }

    /**
     * Submit all work currently in the queues until all workers are busy
     * or all work has been submitted.
     * @return True iff there is no work at the moment.
     */
    private boolean submitAllPossibleTasks()
    {
	boolean nowork;
	Subjob sub = new Subjob();

	if( Settings.traceMasterProgress ){
	    System.out.println( "Master: submitting all possible tasks" );
	}

	while( true ) {
	    synchronized( queue ){
		nowork = queue.selectSubmisson( sub, workers );
		if( nowork || sub.worker == null ){
		    break;
		}
	    }
	    if( Settings.traceMasterQueue ) {
		System.out.println( "Selected " + sub.worker + " as best for task " + sub.task );
	    }
	    submitTaskToWorker( sub );
	}
	return nowork;
    }

    /**
     * Adds the given task to the work queue of this master.
     * @param task The task instance to add to the queue.
     */
    void submit( TaskInstance task )
    {
        if( Settings.traceMasterProgress || Settings.traceMasterQueue) {
            System.out.println( "Master: received task " + task + "; queue length is now " + (1+queue.size()) );
        }
        synchronized ( queue ) {
            incomingTaskCount++;
            queue.submit( task );
        }
        submitAllPossibleTasks();
    }

    /** Runs this master. */
    @Override
    public void run()
    {
	if( Settings.traceMasterProgress ){
	    System.out.println( "Starting master thread" );
	}
	while( true ){
	    boolean nowork = submitAllPossibleTasks();

	    // There are no tasks in the queue, or there are no workers ready.
	    if( nowork && isFinished() ){
	        // No tasks, and we are stopped; don't try to send new tasks.
	        break;
	    }
	    if( Settings.traceMasterProgress ){
		if( nowork ) {
		    System.out.println( "Master: nothing in the queue; waiting" );
		}
		else {
		    System.out.println( "Master: no ready workers; waiting" );		    
		}
	    }
	    // Since the queue is empty, we can only wait for new tasks.
	    try {
		synchronized( queue ){
		    if( !isFinished() ){
			queue.wait();
		    }
		}
	    } catch (InterruptedException e) {
		// Not interested.
	    }
	}
	stopTime = System.nanoTime();
	System.out.println( "End of master thread" );
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    void removeIbis( IbisIdentifier theIbis )
    {
	workers.removeWorker( theIbis );
    }

    /** Returns the identifier of (the receive port of) this worker.
     * 
     * @return The identifier.
     */
    ReceivePortIdentifier identifier()
    {
	return receivePort.identifier();
    }

    /** Print some statistics about the entire master run. */
    void printStatistics( PrintStream s )
    {
	if( stopTime<startTime ) {
	    System.err.println( "Worker didn't stop yet" );
	}
	queue.printStatistics( s );
	long workInterval = stopTime-startTime;
	s.printf(  "Master: # workers        = %5d\n", workers.size() );
	s.printf(  "Master: # incoming tasks = %5d\n", incomingTaskCount );
	s.printf(  "Master: # handled tasks  = %5d\n", handledTaskCount );
	s.println( "Master: run time         = " + Service.formatNanoseconds( workInterval ) );
	sendPort.printStats( s, "master send port" );
	workers.printStatistics( s );
    }

    CompletionInfo[] getCompletionInfo( JobList jobs )
    {
	synchronized( queue ) {
	    return queue.getCompletionInfo( jobs, workers );
	}
    }
}
