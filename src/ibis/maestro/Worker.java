package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

/**
 * A worker in the Maestro multiple master-worker system.
 * @author Kees van Reeuwijk
 */
@SuppressWarnings("synthetic-access")
public final class Worker extends Thread implements WorkSource, PacketReceiveListener<MasterMessage> {

    /** The list of all known masters, in the order that they were handed their
     * id. Thus, element <code>i</code> of the list is guaranteed to have
     * <code>i</code> as its id; otherwise the entry is empty.
     */
    private final ArrayList<MasterInfo> masters = new ArrayList<MasterInfo>();

    /** The list of ibises we haven't registered with yet. */
    private final LinkedList<IbisIdentifier> unregisteredMasters = new LinkedList<IbisIdentifier>();

    /** The list of masters we should tell about new job types we can handle. */
    private final LinkedList<MasterInfo> mastersToUpdate = new LinkedList<MasterInfo>();

    /** The list of masters we should ask for extra work if we are bored. */
    private final ArrayList<MasterInfo> jobSources = new ArrayList<MasterInfo>();

    /** The list of job types we know how to handle. */
    private ArrayList<JobType> jobTypes = new ArrayList<JobType>();

    private final Node node;

    /** The reasoning engine for type support. */
    private final TypeInformation typeAdder;

    private final PacketUpcallReceivePort<MasterMessage> receivePort;
    private final PacketSendPort<WorkerMessage> sendPort;
    private long queueEmptyMoment = 0L;
    private final LinkedList<RunJobMessage> queue = new LinkedList<RunJobMessage>();
    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    private final WorkThread workThreads[] = new WorkThread[numberOfProcessors];
    private boolean stopped = false;
    private final long startTime;
    private long activeTime = 0;
    private long stopTime = 0;
    private long idleDuration = 0;      // Cumulative idle time during the run.
    private long queueDuration = 0;     // Cumulative queue time of all jobs.
    private int jobCount = 0;
    private long workDuration = 0;
    private int runningJobs = 0;
    private int jobSettleCount = 0;
    private boolean askForWork = true;
    private final Random rng = new Random();

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
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + value;
	    return result;
	}

	/**
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
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to.
     * @param node The master that jobs may submit new jobs to.
     * @param typeAdder The types of job this worker can handle.
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis, Node node, TypeInformation typeAdder ) throws IOException
    {
	super( "Worker" );   // Create a thread with a name.
	this.node = node;
	this.typeAdder = typeAdder;
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
	receivePort.enable();   // We're open for business.
    }

    /**
     * Given a job type, records the fact that it can be executed by
     * this worker.
     * @param jobType The allowed job type.
     */
    public void allowJobType( JobType jobType )
    {
	synchronized( queue ) {
	    if( !Service.member( jobTypes, jobType ) ) {
		if( Settings.traceTypeHandling ){
		    Globals.log.reportProgress( "Worker: I can now handle job type " + jobType );
		}
		jobTypes.add( jobType );
		// Now make sure all masters we know get informed about this new job type we support.
		mastersToUpdate.clear();
		for( MasterInfo master: masters ) {
		    if( master.isRegisteredMaster() && !master.isDead() ) {
			mastersToUpdate.add( master );
		    }
		}
		queue.notifyAll();
	    }
	}
    }

    /** The neighbors can now support the given job types.
     * Register any new types this worker can support.
     * @param l A list of supported job types.
     */
    void updateNeighborJobTypes( JobType l[] )
    {
	for( JobType t: l ) {
	    typeAdder.registerNeighborType( node, t );
	}
    }

    /**
     * Set the local listener to the given class instance.
     * @param localListener The local listener to use.
     */
    public void setLocalListener( PacketReceiveListener<WorkerMessage> localListener )
    {
	sendPort.setLocalListener( localListener );
    }

    /** Given a master identifier, returns the master info
     * for the master with that id, or null if there is no such
     * master (any more).
     * @param master The master id to search for.
     * @return The master info, or null if there is no such master.
     */
    private MasterInfo getMasterInfo( MasterIdentifier master )
    {
	synchronized( queue ) {
	    return masters.get( master.value );
	}
    }

    /**
     * Stop this worker.
     */
    public void setStopped()
    {
	synchronized( queue ) {
	    stopped = true;
	    queue.notifyAll();
	}
	System.out.println( "Worker is set to stopped" );
    }

    /**
     * Tells this worker not to ask for work any more.
     */
    public void stopAskingForWork()
    {
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "Worker: don't ask for work" );
	}
	synchronized( queue ){
	    askForWork = false;
	}
    }

    /**
     * Returns the identifier of the job submission port of this worker.
     * @return The port identifier.
     */
    ReceivePortIdentifier identifier()
    {
	return receivePort.identifier();
    }

    /** Removes and returns a random job source from the list of
     * known job sources. Returns null if the list is empty.
     * 
     * @return The job source, or null if there isn't one.
     */
    private MasterInfo getRandomJobSource()
    {
	MasterInfo res;

	synchronized( queue ){
	    while( true ){
		final int size = jobSources.size();
		if( size == 0 ){
		    return null;
		}
		int ix = rng.nextInt( size );
		res = jobSources.remove( ix );
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
    void addJobSource( IbisIdentifier theIbis )
    {
	synchronized( queue ){
	    unregisteredMasters.addLast( theIbis );
	    // This is a good reason to wake up the queue.
	    queue.notifyAll();
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

	synchronized( queue ){
	    // Reserve a slot for this master, and get an id.
	    masterID = new MasterIdentifier( masters.size() );
	    MasterInfo info = new MasterInfo( masterID, ibis );
	    masters.add( info );
	    queue.notifyAll();
	}
	RegisterWorkerMessage msg = new RegisterWorkerMessage( receivePort.identifier(), masterID );
	long sz = sendPort.tryToSend( ibis, Globals.masterReceivePortName, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
	if( sz<0 ) {
	    System.err.println( "Cannot register with master " + ibis );
	    node.declareIbisDead( ibis );
	}
    }

    /** Returns a master to update, or null if there is no such master.
     * 
     * @return The master to update.
     */
    private MasterInfo getMasterToUpdate()
    {
	synchronized( queue ){
	    while( true ) {
		if( mastersToUpdate.isEmpty() ){
		    return null;
		}
		MasterInfo master = mastersToUpdate.removeFirst();
		if( master.isRegisteredMaster() && !master.isDead() ) {
		    return master;
		}
		// Don't return this one, it hasn't accepted yet, or was declared dead.
	    }
	}
    }

    /** Update the given master with our new list of allowed types. */
    private void updateMaster( MasterInfo master )
    {
	JobType jobTypesCopy[];

	synchronized( queue ){
	    jobTypesCopy = new JobType[jobTypes.size()];
	    jobTypes.toArray( jobTypesCopy );
	}
	RegisterTypeMessage msg = new RegisterTypeMessage( master.getIdentifierOnMaster(), jobTypesCopy );
	long sz = sendPort.tryToSend( master.localIdentifier.value, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
	if( sz<0 ) {
	    master.declareDead();
	}
    }

    private void sendJobRequest( MasterInfo master )
    {
	WorkRequestMessage msg = new WorkRequestMessage( master.getIdentifierOnMaster() );
	long sz = sendPort.tryToSend( master.localIdentifier.value, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
	if( sz<0 ) {
	    master.declareDead();
	}
    }

    private void askMoreWork()
    {
	// First, try to tell a master about new job types.
	MasterInfo masterToUpdate = getMasterToUpdate();
	if( masterToUpdate != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: telling master " + masterToUpdate.localIdentifier + " about new job types" );
	    }
	    updateMaster( masterToUpdate );
	    return;
	}

	// Then, try to register with a new ibis.
	IbisIdentifier newIbis = getUnregisteredMaster();
	if( newIbis != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: registering with master " + newIbis );
	    }
	    registerWithMaster( newIbis );
	    return;
	}

	synchronized( queue ){
	    if( !askForWork ){
		return;
	    }
	    jobSettleCount--;
	    if( jobSettleCount>0 ) {
		return;
	    }
	}

	// Finally, try to tell a master we want more jobs.
	MasterInfo jobSource = getRandomJobSource();
	if( jobSource != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: asking master " + jobSource.localIdentifier + " for work" );
	    }
	    sendJobRequest( jobSource );
	    return;
	}
    }

    private void handleWorkerAcceptMessage( WorkerAcceptMessage msg )
    {
	if( Settings.traceWorkerProgress ){
	    Globals.log.reportProgress( "Received a worker accept message " + msg );
	}
	synchronized( queue ){
	    sendPort.registerDestination( msg.port, msg.source.value );
	    MasterInfo master = masters.get( msg.source.value );
	    master.setIdentifierOnMaster( msg.identifierOnMaster );
	    if( Service.member( mastersToUpdate, master ) ){
		Globals.log.reportInternalError( "Master " + master + " is in update list before it accepted this worker??" );
	    }
	    mastersToUpdate.add( master );
	    queue.notifyAll();
	}
    }

    /**
     * Handle a message containing a new job to run.
     * 
     * @param msg The message to handle.
     */
    private void handleRunJobMessage( RunJobMessage msg )
    {
	long now = System.nanoTime();

	msg.setQueueTime( now );
	if( activeTime == 0 ) {
	    activeTime = now;
	}
	synchronized( queue ) {
	    long queueEmptyInterval = 0L;

	    if( queueEmptyMoment>0 ){

		// The queue was empty before we entered this
		// job in it. Register this with this job,
		// so that we can give feedback to the master.
		queueEmptyInterval = now - queueEmptyMoment;
		idleDuration += queueEmptyInterval;
		queueEmptyMoment = 0L;
	    }
	    queue.add( msg );
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
	return port.equals( receivePort.identifier() );
    }

    /**
     * Handles job request message <code>msg</code>.
     * @param msg The job we received and will put in the queue.
     */
    public void messageReceived( MasterMessage msg )
    {
	if( Settings.traceWorkerProgress ){
	    Globals.log.reportProgress( "Worker: received message " + msg );
	}
	if( msg instanceof RunJobMessage ){
	    RunJobMessage runJobMessage = (RunJobMessage) msg;

	    handleRunJobMessage( runJobMessage );
	}
	else if( msg instanceof WorkerAcceptMessage ) {
	    WorkerAcceptMessage am = (WorkerAcceptMessage) msg;

	    handleWorkerAcceptMessage( am );
	}
	else {
	    Globals.log.reportInternalError( "FIXME: handle messages of type " + msg.getClass() );
	}
    }

    /** Gets a job to execute.
     * @return The next job to execute.
     */
    @Override
    public RunJobMessage getJob()
    {
	while( true ) {
	    boolean askForWork = false;
	    try {
		synchronized( queue ) {
		    if( queue.isEmpty() ) {
			if( queueEmptyMoment == 0 ) {
			    queueEmptyMoment = System.nanoTime();
			}
			if( stopped && runningJobs == 0 ) {
			    // No jobs in queue, and worker is stopped. Return null to
			    // indicate that there won't be further jobs.
			    break;
			}
			if( jobSources.isEmpty() && unregisteredMasters.isEmpty() && mastersToUpdate.isEmpty() ){
			    // There was no master to subscribe to, update, or ask for work.
			    if( Settings.traceWorkerProgress ) {
				System.out.println( "Worker: waiting for new jobs in queue" );
			    }
			    queue.wait();
			}
			else {
			    askForWork = true;
			}
		    }
		    else {
			runningJobs++;
			RunJobMessage job = queue.remove();
			long now = System.nanoTime();
			job.setRunTime( now );
			if( Settings.traceWorkerProgress ) {
			    System.out.println( "Worker: handed out job " + job + "; it was queued for " + Service.formatNanoseconds( now-job.getQueueTime() ) + "; there are now " + runningJobs + " running jobs" );
			}
			if( jobSettleCount>0 ) {
			    jobSettleCount--;
			}
			return job;
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

    /** Reports the result of the execution of a job. (Overrides method in superclass.)
     * @param jobMessage The job that was run.
     */
    @Override
    public void reportJobCompletion( RunJobMessage jobMessage )
    {
	long now = System.nanoTime();
	long queueInterval = jobMessage.getRunTime()-jobMessage.getQueueTime();

	WorkerMessage msg = new WorkerStatusMessage( jobMessage.workerIdentifier, jobMessage.jobId, queueInterval );
	final MasterIdentifier master = jobMessage.source;
	long sz = sendPort.tryToSend( master.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "Completed job "  + jobMessage );
	}
	final MasterInfo mi = getMasterInfo( master );
	synchronized( queue ) {
	    if( mi != null ) {
		if( !Service.member( jobSources, mi ) ) {
		    jobSources.add( mi );
		}
	    }
	    queueDuration += queueInterval;
	    workDuration += now-jobMessage.getRunTime();
	    jobCount++;
	    runningJobs--;
	    queue.notifyAll();
	}
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis( IbisIdentifier theIbis )
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
    public void printStatistics()
    {
	if( stopTime<startTime ) {
	    System.err.println( "Worker didn't stop yet" );
	    stopTime = System.nanoTime();
	}
	if( activeTime<startTime ) {
	    System.err.println( "Worker was not used" );
	    activeTime = startTime;
	}
	long workInterval = stopTime-activeTime;
	double idlePercentage = 100.0*((double) idleDuration/(double) workInterval);
	double workPercentage = 100.0*((double) workDuration/(double) workInterval);
	System.out.printf( "Worker: # threads        = %5d\n", workThreads.length );
	System.out.printf( "Worker: # jobs           = %5d\n", jobCount );
	System.out.println( "Worker: run time         = " + Service.formatNanoseconds( workInterval ) );
	System.out.println( "Worker: activated after  = " + Service.formatNanoseconds( activeTime-startTime ) );
	System.out.println( "Worker: total work time  = " + Service.formatNanoseconds( workDuration ) + String.format( " (%.1f%%)", workPercentage )  );
	System.out.println( "Worker: total idle time  = " + Service.formatNanoseconds( idleDuration ) + String.format( " (%.1f%%)", idlePercentage ) );
	sendPort.printStats( "worker send port" );
	if( jobCount>0 ) {
	    System.out.println( "Worker: queue time/job   = " + Service.formatNanoseconds( queueDuration/jobCount ) );
	    System.out.println( "Worker: compute time/job = " + Service.formatNanoseconds( workDuration/jobCount ) );
	}
    }

    /**
     * Send a result message to the given port, using the given task identifier
     * and the given result value.
     * @param port The port to send the result to.
     * @param id The task identifier.
     * @param result The result to send.
     * @return The size of the sent message, or -1 if the transmission failed.
     */
    public long sendResultMessage(ReceivePortIdentifier port, TaskIdentifier id,
	    JobResultValue result) {
	WorkerMessage msg = new ResultMessage( id, result );
	return sendPort.tryToSend( port, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
    }
}
