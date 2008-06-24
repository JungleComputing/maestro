package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 * Information about a worker in the list of a master.
 */
final class WorkerInfo {
    /** Our identifier with this worker. */
    final MasterIdentifier identifierWithWorker;

    /** The active tasks of this worker. */
    private final ArrayList<ActiveTask> activeTasks = new ArrayList<ActiveTask>();

    private final Hashtable<TaskType,WorkerTaskInfo> workerTaskInfoTable = new Hashtable<TaskType, WorkerTaskInfo>();

    /** Our local identifier of this worker. */
    final Master.WorkerIdentifier identifier;

    /** The receive port of the worker. */
    final ReceivePortIdentifier port;

    final boolean local;

    private boolean enabled = false;

    private boolean dead = false;

    /**
     * Returns a string representation of this worker info. (Overrides method in superclass.)
     * @return The worker info.
     */
    @Override
    public String toString()
    {
	return "Worker " + identifier;
    }

    WorkerInfo( ReceivePortIdentifier port, WorkerIdentifier identifier, MasterIdentifier identifierForWorker, boolean local, TaskType[] types )
    {
	this.port = port;
	this.identifier = identifier;
	this.identifierWithWorker = identifierForWorker;
	this.local = local;
	for( TaskType t: types ) {
	    registerTaskType(t);
	}
    }

    /**
     * Given a task identifier, returns the task queue entry with that id, or null.
     * @param id The task identifier to search for.
     * @return The index of the ActiveTask with this id, or -1 if there isn't one.
     */
    private int searchActiveTask( long id )
    {
        // Note that we blindly assume that there is only one entry with
        // the given id. Reasonable because we hand out the ids ourselves,
        // and we never make mistakes...
        for( int ix=0; ix<activeTasks.size(); ix++ ) {
            ActiveTask e = activeTasks.get( ix );
	    if( e.id == id ) {
		return ix;
	    }
	}
	return -1;
    }

    private void registerCompletionInfo( CompletionInfo completionInfo )
    {
        if( completionInfo == null ) {
            return;
        }
	WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( completionInfo.type );

	if( workerTaskInfo == null ) {
	    return;
	}
	if( completionInfo.completionInterval != Long.MAX_VALUE ) {
	    workerTaskInfo.setCompletionInterval( completionInfo.completionInterval );
	}
    }

    void registerCompletionInfo( CompletionInfo[] completionInfo )
    {
        if( Settings.traceWorkerList && !enabled ){
            System.out.println( "Enabled worker " + identifier + " (" + port + ")" );
        }
        enabled = true;
	for( CompletionInfo i: completionInfo ) {
	    registerCompletionInfo( i );
	}
    }

    private void registerWorkerQueueInfo( WorkerQueueInfo info )
    {
        if( info == null ) {
            return;
        }
	WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( info.type );

	if( workerTaskInfo == null ) {
	    return;
	}
	workerTaskInfo.controlAllowance( info.queueLength );

    }

    void registerWorkerQueueInfo( WorkerQueueInfo[] completionInfo )
    {
        if( Settings.traceWorkerList && !enabled ){
            System.out.println( "Enabled worker " + identifier + " (" + port + ")" );
        }
        enabled = true;
	for( WorkerQueueInfo i: completionInfo ) {
	    registerWorkerQueueInfo( i );
	}
    }

    /**
     * Register a task result for an outstanding task.
     * @param result The task result message that tells about this task.
     */
    void registerWorkerStatus( TaskCompletedMessage result )
    {
	final long id = result.taskId;    // The identifier of the task, as handed out by us.

	long now = System.nanoTime();
	int ix = searchActiveTask( id );
	if( ix<0 ) {
	    Globals.log.reportInternalError( "Master: ignoring reported result from task with unknown id " + id );
	    return;
	}
	ActiveTask task = activeTasks.remove( ix );
	long newRoundTripInterval = (now-task.startTime); // The time interval to send the task, compute, and report the result.

	registerCompletionInfo( result.completionInfo );
	registerWorkerQueueInfo( result.workerQueueInfo );
	task.workerTaskInfo.registerTaskCompleted( newRoundTripInterval );
	//limitQueueTime( task.workerTaskInfo, newRoundTripInterval, queueInterval+result.computeInterval );
	if( Settings.traceMasterProgress ){
	    System.out.println( "Master: retired task " + task + "; roundTripTime=" + Service.formatNanoseconds( newRoundTripInterval ) );
	}
    }

    /**
     * Returns true iff this worker is idle.
     * @return True iff this worker is idle.
     */
    boolean isIdle()
    {
	return enabled && activeTasks.isEmpty();
    }

    /** Register the start of a new task.
     * 
     * @param task The task that was started.
     * @param id The id given to the task.
     */
    void registerTaskStart( TaskInstance task, long id )
    {
	WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( task.type );
	if( workerTaskInfo == null ) {
	    System.err.println( "No worker task info for task type " + task.type );
	    return;
	}
	workerTaskInfo.incrementOutstandingTasks();
	ActiveTask j = new ActiveTask( task, id, System.nanoTime(), workerTaskInfo );

	activeTasks.add( j );
    }

    /** Given a task id, retract it from the administration.
     * For some reason we could not send this task to the worker.
     * @param id The identifier of the task.
     */
    void retractTask( long id )
    {
        int ix = searchActiveTask( id );
        if( ix<0 ) {
            Globals.log.reportInternalError( "Master: ignoring task retraction for unknown id " + id );
            return;
        }
        ActiveTask task = activeTasks.remove( ix );
	System.out.println( "Master: retracted task " + task );
    }

    /**
     * @param type The task type to register for.
     */
    private void registerTaskType( TaskType type )
    {
        if( Settings.traceTypeHandling ){
            System.out.println( "worker " + identifier + " (" + port + ") can handle " + type );
        }
	WorkerTaskInfo info = new WorkerTaskInfo( toString() + " task type " + type, type.remainingTasks, local );
	workerTaskInfoTable.put( type, info );
    }

    /** Given a task type, estimate the completion time of this task type,
     * or Long.MAX_VALUE if the task type is not allowed on this worker,
     * or the worker is currently using its entire allowance.
     * @param taskType The task type for which we want to know the round-trip interval.
     * @return The interval, or Long.MAX_VALUE if this type of task is not allowed.
     */
    long estimateJobCompletion( TaskType taskType )
    {
        if( !enabled ) {
            if( Settings.traceTypeHandling ){
                System.out.println( "estimateJobCompletion(): worker " + identifier + " not yet enabled" );
            }
            return Long.MAX_VALUE;
        }
	WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( taskType );
	if( workerTaskInfo == null ) {
	    if( Settings.traceTypeHandling ){
	        System.out.println( "estimateJobCompletion(): worker " + identifier + " does not support type " + taskType );
	    }
	    return Long.MAX_VALUE;
	}
	return workerTaskInfo.estimateJobCompletion();
    }

    long getAverageCompletionTime( TaskType taskType )
    {
	WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( taskType );

	if( workerTaskInfo == null ) {
	    if( Settings.traceTypeHandling ){
		Globals.log.reportProgress( "getAverageCompletionTime(): worker " + identifier + " does not support type " + taskType );
	    }
	    return Long.MAX_VALUE;
	}
        return workerTaskInfo.getAverageCompletionTime();
    }

    /**
     * Tries to increment the maximal number of outstanding tasks for this worker.
     * @param taskType The task type for which we want to increase our allowance.
     * @return True iff we could increment the allowance of this type.
     */
    protected boolean incrementAllowance( TaskType taskType )
    {
	WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( taskType );
	if( workerTaskInfo == null ) {
	    return false;
	}
	return workerTaskInfo.incrementAllowance();
    }

    /**
     * Returns true iff this worker is dead.
     * @return Is this worker dead?
     */
    boolean isDead()
    {
	return dead;
    }

    /** Mark this worker as dead.
     * 
     */
    void setDead()
    {
	dead = true;
    }

    /**
     * Given a task type, return true iff this worker
     * supports the task type.
     * @param type The task type we're looking for.
     * @return True iff this worker supports the type.
     */
    boolean supportsType( TaskType type )
    {
	WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( type );
	final boolean res = workerTaskInfo != null;

        if( Settings.traceTypeHandling ){
            System.out.println( "Worker " + identifier + " supports type " + type + "? Answer: " + res );
        }
	return res;
    }

    /**
     * Given a print stream, print some statistics about this worker.
     * @param s The stream to print to.
     */
    void printStatistics( PrintStream s )
    {
        Enumeration<TaskType> keys = workerTaskInfoTable.keys();
	s.println( "Worker " + identifier + (local?" (local)":"") );

        while( keys.hasMoreElements() ){
	    TaskType taskType = keys.nextElement();
	    WorkerTaskInfo info = workerTaskInfoTable.get( taskType );
	    if( info.didWork() ) {
	        String stats = info.buildStatisticsString();
	        s.println( "  " + taskType.toString() + ": " + stats );
	    }
	}
    }
}
