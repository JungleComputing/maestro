package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Task.TaskIdentifier;

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
public final class Worker extends Thread implements JobSource, PacketReceiveListener<MasterMessage> {

    /** The list of all known masters, in the order that they were handed their
     * id. Thus, element <code>i</code> of the list is guaranteed to have
     * <code>i</code> as its id.
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

    private TaskList tasks = new TaskList();
    private static int taskCounter = 0;

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
    private int runningJobs = 0;
    private boolean askMastersForWork = true;
    private final Random rng = new Random();

    private static class JobStats {
	private int jobCount = 0;
	private long workDuration = 0;        
	private long queueDuration = 0;     // Cumulative queue time of all jobs.

	/**
	 * Registers the completion of a job of this particular type, with the
	 * given queue interval and the given work interval.
	 * @param queueInterval The time this job spent in the queue.
	 * @param workInterval The time it took to execute this job.
	 */
	private void countJob( long queueInterval, long workInterval )
	{
	    jobCount++;
	    queueDuration += queueInterval;
	    workDuration += workInterval;
	}

	private void reportStats( PrintStream out, JobType t, double workInterval )
	{
	    double workPercentage = 100.0*(workDuration/workInterval);
	    if( jobCount>0 ) {
		out.println( "Worker: " + t + ":" );
		out.printf( "    # jobs          = %5d\n", jobCount );
		out.println( "    total work time = " + Service.formatNanoseconds( workDuration ) + String.format( " (%.1f%%)", workPercentage )  );
		out.println( "    queue time/job  = " + Service.formatNanoseconds( queueDuration/jobCount ) );
		out.println( "    work time/job   = " + Service.formatNanoseconds( workDuration/jobCount ) );
		out.println( "    average latency = " + Service.formatNanoseconds( (workDuration+queueDuration)/jobCount ) );
	    }
	    else {
		out.println( "Worker: " + t + " is unused" );
	    }
	}
    }

    private HashMap<JobType, JobStats> jobStats = new HashMap<JobType, JobStats>();

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
     * @throws IOException Thrown if the construction of the worker failed.
     */
    Worker( Ibis ibis, Node node ) throws IOException
    {
	super( "Worker" );   // Create a thread with a name.
	this.node = node;
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
    }

    /**
     * Given a job type, records the fact that it can be executed by
     * this worker.
     * @param jobType The allowed job type.
     */
    void allowJobType( JobType jobType )
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
		    if( master.isRegistered() && !master.isDead() ) {
			mastersToUpdate.add( master );
		    }
		}
		queue.notifyAll();
	    }
	}
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
	System.out.println( "Worker is set to stopped" );
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
    private MasterInfo getRandomWorkSource()
    {
	MasterInfo res;

	synchronized( queue ){
	    if( !askMastersForWork ){
	        return null;
	    }
	    while( true ){
	        int size = jobSources.size();
	        if( size == 0 ){
	            // Nothing in the job sources; just draw a random master
	            // from the list of known masters.
	            size = masters.size();
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
	        // There are masters on the explict job sources list,
	        // draw a random one.
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
	    if( activeTime == 0 || queueEmptyMoment != 0 ) {
	        // We haven't done any work yet, or we are idle.
	        queue.notifyAll();
	    }
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
		if( master.isRegistered() && !master.isDead() ) {
		    return master;
		}
		// Don't return this one, it hasn't accepted yet, or was declared dead.
	    }
	}
    }

    /** Update the given master with our new list of allowed types.
     * @param master The master to update.
     * @param completionInfo Completion times for the different job types from this master.
     */
    private void updateMaster( MasterInfo master, CompletionInfo[] completionInfo )
    {
	JobType jobTypesCopy[];

	synchronized( queue ){
	    jobTypesCopy = new JobType[jobTypes.size()];
	    jobTypes.toArray( jobTypesCopy );
	}
	RegisterTypeMessage msg = new RegisterTypeMessage( master.getIdentifierOnMaster(), completionInfo, jobTypesCopy );
	long sz = sendPort.tryToSend( master.localIdentifier.value, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
	if( sz<0 ) {
	    master.declareDead();
	}
    }

    private void sendJobRequest( MasterInfo master, CompletionInfo completionInfo[] )
    {
	WorkRequestMessage msg = new WorkRequestMessage( master.getIdentifierOnMaster(), completionInfo );
	long sz = sendPort.tryToSend( master.localIdentifier.value, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
	if( sz<0 ) {
	    master.declareDead();
	}
    }

    private void askMoreWork( CompletionInfo[] completionInfo )
    {
	// First, try to tell a master about new job types.
	MasterInfo masterToUpdate = getMasterToUpdate();
	if( masterToUpdate != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: telling master " + masterToUpdate.localIdentifier + " about new job types" );
	    }
	    updateMaster( masterToUpdate, completionInfo );
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

	// Finally, try to tell a master we want more jobs.
	MasterInfo jobSource = getRandomWorkSource();
	if( jobSource != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: asking master " + jobSource.localIdentifier + " for work" );
	    }
	    sendJobRequest( jobSource, completionInfo );
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
		Globals.log.reportInternalError( "Master " + master + " already was in update list before it accepted this worker??" );
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
	synchronized( queue ) {
            if( activeTime == 0 ) {
                activeTime = now;
            }
	    if( queueEmptyMoment>0 ){
		// The queue was empty before we entered this
		// job in it. Register this with this job,
		// so that we can give feedback to the master.
		long queueEmptyInterval = now - queueEmptyMoment;
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
    public RunJob getJob()
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
			RunJobMessage message = queue.remove();
			long now = System.nanoTime();
			message.setRunTime( now );
                        Job job = findJob( message.job.type );
			if( Settings.traceWorkerProgress ) {
			    System.out.println( "Worker: handed out job " + message + " of type " + message.job.type + "; it was queued for " + Service.formatNanoseconds( now-message.getQueueTime() ) + "; there are now " + runningJobs + " running jobs" );
			}
                        // TODO: also pass on the task in RunJob.
			return new RunJob( job, message );
		    }
		}
		if( askForWork ){
		    CompletionInfo[] completionInfo = node.getCompletionInfo( tasks );
		    askMoreWork( completionInfo );
		}
	    }
	    catch( InterruptedException e ){
		// Not interesting.
	    }
	}
	return null;
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

    /**
     * Given a job type, return the task it belongs to, or <code>null</code> if we
     * cannot find it. Since that is an internal error, report that error.
     * @param type
     * @return
     */
    private Task findTask( JobType type )
    {
        int ix = searchTask( type.task );
        if( ix<0 ) {
            Globals.log.reportInternalError( "Unknown task id in job type " + type );
            return null;
        }
        return tasks.get( ix );
    }

    /**
     * Given a job type, return the job.
     * @param type The job type.
     * @return The job.
     */
    private Job findJob( JobType type )
    {
        Task t = findTask( type );
        return t.jobs[type.jobNo];
    }

    /** Reports the result of the execution of a job. (Overrides method in superclass.)
     * @param job The job that was run.
     * @param result The result coming rom the run job.
     */
    @Override
    public void reportJobCompletion( RunJob job, Object result )
    {
	long now = System.nanoTime();
	long queueInterval = job.message.getRunTime()-job.message.getQueueTime();
	long computeInterval = job.message.getRunTime()-now;
	JobType jobType = job.message.job.type;
        Task t = findTask( jobType );
        int nextJobNo = jobType.jobNo+1;

        long taskCompletionInterval;
        if( nextJobNo<t.jobs.length ){
            // There is a next step to take.
            JobInstance nextJob = new JobInstance( job.message.job.taskInstance, new JobType( jobType.task, nextJobNo ), result );
            taskCompletionInterval = node.submitAndGetInfo( nextJob );
        }
        else {
            // This was the final step. Report back the result.
            TaskInstanceIdentifier identifier = job.message.job.taskInstance;
            sendResultMessage( identifier.receivePort, identifier, result );
            taskCompletionInterval = 0L;
        }
        if( Settings.traceRemainingTaskTime ) {
            Globals.log.reportProgress( "Completed " + job.message + "; queueInterval=" + Service.formatNanoseconds( queueInterval ) + " taskCompletionInterval=" + Service.formatNanoseconds( taskCompletionInterval ) );
        }
	final MasterIdentifier master = job.message.source;

        // Update statistics and notify our own queue waiters that something
        // has happened.
	synchronized( queue ) {
	    final MasterInfo mi = masters.get( master.value );
	    if( mi != null ) {
		if( !Service.member( jobSources, mi ) ) {
		    jobSources.add( mi );
		}
	    }
	    if( !jobStats.containsKey(jobType) ){
		jobStats.put( jobType, new JobStats() );
	    }
	    JobStats stats = jobStats.get( jobType );
	    stats.countJob( queueInterval, now-job.message.getRunTime() );
	    runningJobs--;
	    //queue.updateCompletionInterval( jobType, taskCompletionInterval );
	    //completionInfo = queue.getCompletionInfo();
	    queue.notifyAll();
	}
	CompletionInfo[] completionInfo = node.getCompletionInfo( tasks );	
	WorkerMessage msg = new JobCompletedMessage( job.message.workerIdentifier, job.message.jobId, queueInterval, computeInterval, completionInfo );
	long sz = sendPort.tryToSend( master.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "Completed job "  + job.message );
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
        tasks.printStatistics( s );
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
	Set<JobType> tl = jobStats.keySet();
	for( JobType t: tl ){
	    JobStats stats = jobStats.get( t );
	    stats.reportStats( s, t, workInterval );
	}
	s.printf( "Worker: # threads        = %5d\n", workThreads.length );
	s.println( "Worker: run time         = " + Service.formatNanoseconds( workInterval ) );
	s.println( "Worker: activated after  = " + Service.formatNanoseconds( activeTime-startTime ) );
	s.println( "Worker: total idle time  = " + Service.formatNanoseconds( idleDuration ) + String.format( " (%.1f%%)", idlePercentage ) );
	sendPort.printStats( s, "worker send port" );
    }

    /**
     * Send a result message to the given port, using the given task identifier
     * and the given result value.
     * @param port The port to send the result to.
     * @param id The task identifier.
     * @param result The result to send.
     * @return The size of the sent message, or -1 if the transmission failed.
     */
    long sendResultMessage(ReceivePortIdentifier port, TaskInstanceIdentifier id,
	    Object result) {
	WorkerMessage msg = new TaskResultMessage( id, result );
	return sendPort.tryToSend( port, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
    }

    /**
     * Register a new task.
     * 
     * @param task The task to register.
     */
    void registerTask( Task task )
    {
	TaskIdentifier id = task.id;
	Job jobs[] = task.jobs;

	for( int i=0; i<jobs.length; i++ ){
	    Job j = jobs[i];
	    if( j.isSupported() ) {
		allowJobType( new JobType( id, i ) );
	    }
	}
    }

    /**
     * Creates a task with the given name and the given sequence of jobs.
     * 
     * @param name The name of the task.
     * @param jobs The list of jobs of the task.
     * @return A new task instance representing this task.
     */
    public Task createTask( String name, Job jobs[] )
    {
        int taskId = taskCounter++;
        Task task = new Task( node, taskId, name, jobs );

        tasks.add( task );
        registerTask( task );
        return task;
    }
}
