package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

/**
 * A worker in the Maestro workflow framework.
 * @author Kees van Reeuwijk
 */
@SuppressWarnings("synthetic-access")
public final class Worker
{
    private long activeTime = 0L;

    /** The list of ibises we haven't registered with yet. */
    final MasterInfoList unregisteredNodes = new MasterInfoList();

    /** The list of masters we should ask for extra work if we are bored. */
    private final ArrayList<NodeInfo> taskSources = new ArrayList<NodeInfo>();

    final Node node;

    private long queueEmptyMoment = 0L;
    final WorkerQueue queue = new WorkerQueue();
    private boolean stopped = false;
    private long idleDuration = 0;      // Cumulative idle time during the run.
    private int runningTasks = 0;
    private boolean askMastersForWork = true;
    private final Random rng = new Random();

    private final boolean traceStats;

    ArrayList<WorkerTaskStats> taskStats = new ArrayList<WorkerTaskStats>();

    static final class MasterIdentifier implements Serializable {
	private static final long serialVersionUID = 7727840589973468928L;
	final int value;

	MasterIdentifier( int value )
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
     * @param node The node this worker belongs to.
     */
    Worker( Node node )
    {
	this.node = node;
	this.traceStats = System.getProperty( "ibis.maestro.traceWorkerStatistics" ) != null;
	long now = System.nanoTime();
	queueEmptyMoment = now;
    }

    /**
     * Tells this worker not to ask for work any more.
     */
    void stopAskingForWork()
    {
	if( Settings.traceWorkerProgress ) {
	    Globals.log.reportProgress( "Worker: don't ask for work" );
	}
	synchronized( queue ){
	    askMastersForWork = false;
	}
    }

    /** Removes and returns a random task source from the list of
     * known task sources. Returns null if the list is empty.
     * 
     * @return The task source, or null if there isn't one.
     */
    private NodeInfo getRandomWorkSource()
    {
	NodeInfo res;

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
		if( !res.isSuspect() ){
		    return res;
		}
		// The master we drew from the lottery is suspect or dead. Try again.
		// Note that the suspect task source has been removed from the list.
	    }
	}
    }

    /**
     * Returns a random registered master.
     * @return The task source, or <code>null</code> if there isn't one.
     */
    private NodeInfo getRandomRegisteredMaster()
    {
	NodeInfo res;

	synchronized( queue ){
	    if( !askMastersForWork ){
		return null;
	    }
	    int size = node.masters.size();
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
		res = node.masters.get( ix );
		if( !res.isSuspect() && res.isRegistered() ) {
		    return res;
		}
		ix++;
		if( ix>=node.masters.size() ) {
		    // Wrap around.
		    ix = 0;
		}
		n--;
	    }
	    // We tried all elements in the list with no luck.
	    return null;
	}
    }

    /**
     * If there is any new master on our list, try to register with it.
     */
    private void registerWithAnyMaster()
    {
	NodeInfo newIbis = unregisteredNodes.removeIfAny();
	if( newIbis != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: registering with master " + newIbis );
	    }
	    // We record the transmission time as a reasonable estimate of a sleep time.
	    long start = System.nanoTime();
	    boolean ok = node.registerWithNode( newIbis );
	    if( ok ) {
		node.infoSendTime.addSample( System.nanoTime()-start );
	    }
	    else {
		// We couldn't reach this master. Put it back on the list.
		unregisteredNodes.add( newIbis );
	    }
	    return;
	}
    }

    private void askMoreWork()
    {
	// Try to tell a known master we want more tasks. We do this by
	// telling it about our current state.
	NodeInfo taskSource = getRandomWorkSource();
	if( taskSource != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: asking master " + taskSource.localIdentifier + " for work" );
	    }
	    long start = System.nanoTime();
	    node.sendUpdate( taskSource );
	    node.infoSendTime.addSample( System.nanoTime()-start );
	    return;
	}

	// Finally, just tell a random master about our current work queues.
	taskSource = getRandomRegisteredMaster();
	if( taskSource != null ){
	    if( Settings.traceWorkerProgress ){
		Globals.log.reportProgress( "Worker: updating master " + taskSource.localIdentifier );
	    }
	    long start = System.nanoTime();
	    node.sendUpdate( taskSource );
	    node.infoSendTime.addSample( System.nanoTime()-start );
	    return;
	}
    }

    /**
     * Handle a message containing a new task to run.
     * 
     * @param msg The message to handle.
     * @param arrivalMoment The moment in ns this message arrived.
     */
    void handleRunTaskMessage( RunTaskMessage msg, long arrivalMoment )
    {
	NodeInfo master;
	NodeInfo unsuspect = null;

	synchronized( queue ) {
	    if( activeTime == 0L ) {
		activeTime = arrivalMoment;
	    }
	    if( queueEmptyMoment>0L ){
		// The queue was empty before we entered this
		// task in it. Record this for the statistics.
		long queueEmptyInterval = arrivalMoment - queueEmptyMoment;
		idleDuration += queueEmptyInterval;
		queueEmptyMoment = 0L;
	    }
	    int length = queue.add( msg );
	    msg.setQueueMoment( arrivalMoment, length );
	    master = node.masters.get( msg.source.value );
	    if( master != null && master.isSuspect() && !master.isDead() ) {
		master.setUnsuspect();
		unsuspect = master;
	    }
	}
	if( master != null ) {
	    node.sendUpdate( master );
	}
	if( unsuspect != null ) {
	    node.setUnsuspectOnMaster( unsuspect.ibis );
	}
    }

    private WorkerTaskStats getWorkerTaskStats( TaskType type )
    {
	int ix = type.index;
	WorkerTaskStats res;
	while( ix>=taskStats.size() ) {
	    taskStats.add( null );
	}
	res = taskStats.get( ix );
	if( res == null ){
	    res = new WorkerTaskStats( type );
	    taskStats.set( ix, res );
	}
	return res;
    }

    /** Reports the result of the execution of a task. (Overrides method in superclass.)
     * @param task The task that was run.
     * @param result The result coming from the run task.
     */
    public void reportTaskCompletion( RunTask task, Object result )
    {
	long taskCompletionMoment = System.nanoTime();
	TaskType taskType = task.message.task.type;
	Job t = node.findJob( taskType );
	int nextTaskNo = taskType.taskNo+1;
	final MasterIdentifier master = task.message.source;

	CompletionInfo[] completionInfo = node.master.getCompletionInfo( node.jobs, node.nodes );
	WorkerQueueInfo[] workerQueueInfo = queue.getWorkerQueueInfo( taskStats );
	long workerDwellTime = taskCompletionMoment-task.message.getQueueMoment();
	if( traceStats ) {
	    double now = 1e-9*(System.nanoTime()-node.startTime);
	    System.out.println( "TRACE:workerDwellTime " + taskType + " " + now + " " + 1e-9*workerDwellTime );
	}
	Message msg = new TaskCompletedMessage( task.message.workerIdentifier, task.message.taskId, workerDwellTime, completionInfo, workerQueueInfo );
	long sz = node.sendPort.tryToSend( master.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );

	// FIXME: try to do something if we couldn't send to the originator of the job. At least retry.

	if( nextTaskNo<t.tasks.length ){
	    // There is a next step to take.
	    TaskType nextTaskType = t.getNextTaskType( taskType );
	    TaskInstance nextTask = new TaskInstance( task.message.task.jobInstance, nextTaskType, result );
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
	    final NodeInfo mi = node.masters.get( master.value );
	    if( mi != null ) {
		if( !taskSources.contains( mi ) ) {
		    taskSources.add( mi );
		}
	    }
	    WorkerTaskStats stats = getWorkerTaskStats( taskType );
	    long queueInterval = task.message.getRunMoment()-task.message.getQueueMoment();
	    stats.countTask( queueInterval, taskCompletionMoment-task.message.getRunMoment() );
	    runningTasks--;
	    if( Settings.traceRemainingJobTime ) {
		Globals.log.reportProgress( "Completed " + task.message.task + "; queueInterval=" + Service.formatNanoseconds( queueInterval ) + "; runningTasks=" + runningTasks );
	    }
	}
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "Completed task "  + task.message );
	}
    }

    /** Print some statistics about the entire worker run. */
    void printStatistics( PrintStream s )
    {
	if( activeTime<node.startTime ) {
	    System.err.println( "Worker was not used" );
	    activeTime = node.startTime;
	}
	long workInterval = node.stopTime-activeTime;
	queue.printStatistics( s );
	double idlePercentage = 100.0*((double) idleDuration/(double) workInterval);
	for( WorkerTaskStats stats: taskStats ) {
	    if( stats != null ) {
		stats.reportStats( s, workInterval );
	    }
	}
	s.println( "Worker: run time        = " + Service.formatNanoseconds( workInterval ) );
	s.println( "Worker: activated after = " + Service.formatNanoseconds( activeTime-node.startTime ) );
	s.println( "Worker: total idle time = " + Service.formatNanoseconds( idleDuration ) + String.format( " (%.1f%%)", idlePercentage ) );
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * @param port The port to send the result to.
     * @param id The job instance identifier.
     * @param result The result to send.
     * @return The size of the sent message, or -1 if the transmission failed.
     */
    long sendResultMessage( ReceivePortIdentifier port, JobInstanceIdentifier id,
	    Object result ) {
	Message msg = new JobResultMessage( id, result );
	return node.sendPort.tryToSend( port, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
    }
    /** Gets a task to execute.
     * @return The next task to execute.
     */
    RunTask getTask()
    {
        while( true ) {
            boolean askForWork = false;
            registerWithAnyMaster();
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
                        if( taskSources.isEmpty() ){
                            // There was no master to subscribe to, update, or ask for work.
                            if( Settings.traceWorkerProgress || Settings.traceWaits ) {
                                System.out.println( "Worker: waiting for new tasks in queue" );
                            }
                            // Wait a little if there is nothing to do.
                            queue.wait( (node.infoSendTime.getAverage()*2)/Service.MILLISECOND_IN_NANOSECONDS );
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
                        WorkerTaskStats stats = getWorkerTaskStats( type );
                        stats.setQueueTimePerTask( queueTime/(queueLength+1) );
                        Task task = node.findTask( message.task.type );
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

    /** FIXME.
     * 
     */
    void updateAdministration()
    {
        // FIXME Edit this auto-generated method stub
        
    }
}
