package ibis.maestro;

import ibis.ipl.IbisIdentifier;
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

    /** The time the accept message was sent to this worker.
     * The roundtrip time determines the ping duration of this worker.
     */
    private long pingSentTime = 0;

    /** The duration of the ping round-trip for this worker. */
    private long pingTime;

    private int missedDeadlines = 0;
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
        // Initial, totally unfounded, guess for the ping time.
        this.pingTime = local?Service.MICROSECOND_IN_NANOSECONDS:Service.MILLISECOND_IN_NANOSECONDS;
        for( TaskType t: types ) {
            registerTaskType( t );
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
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Master: worker " + identifier + ":" + completionInfo );
        }
        if( completionInfo.completionInterval != Long.MAX_VALUE ) {
            workerTaskInfo.setCompletionTime( completionInfo.completionInterval );
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
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Master: worker " + identifier + ":" + info );
        }
        workerTaskInfo.setDwellTime( info.workerDwellTime );
        workerTaskInfo.controlAllowance( info.queueLength );
    }

    /** Update all task info to take into account the given ping time.
     * @param table The task info table to update.
     * @param pingTime The ping time to this worker.
     */
    private static void setPingTime( Hashtable<TaskType,WorkerTaskInfo> table, long pingTime )
    {
        Enumeration<TaskType> keys = table.keys();

        while( keys.hasMoreElements() ){
            TaskType t = keys.nextElement();
            WorkerTaskInfo wi = table.get( t );
            wi.setPingTime( pingTime );
        }        
    }

    void registerWorkerInfo( WorkerQueueInfo[] workerQueueInfo, CompletionInfo[] completionInfo, long arrivalMoment )
    {
        if( Settings.traceWorkerList && !enabled ){
            System.out.println( "Enabled worker " + identifier + " (" + port + ")" );
        }
        if( pingSentTime != 0 ) {
            // We are measuring this round-trip time.
            pingTime = arrivalMoment-pingSentTime;
            pingSentTime = 0L;  // We're no longer measuring a ping time.
            setPingTime( workerTaskInfoTable, pingTime );
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportProgress( "Master: ping time to worker " + identifier + " is " + Service.formatNanoseconds( pingTime ) );
            }
        }
        enabled = true;
        if( Settings.traceRemainingJobTime ) {
            String s = "workerQueueInfo=[";
            boolean first = true;
            for( WorkerQueueInfo i: workerQueueInfo ) {
                if( first ) {
                    first = false;
                }
                else {
                    s += ",";
                }
                s += i;
            }
            s += "] completionInfo=[";
            first = true;
            for( CompletionInfo i: completionInfo ) {
                if( first ) {
                    first = false;
                }
                else {
                    s += ",";
                }
                s += i;
            }
            s += ']';
            Globals.log.reportProgress( "Master: got new information from " + identifier + ": " +  s );
        }
        for( WorkerQueueInfo i: workerQueueInfo ) {
            registerWorkerQueueInfo( i );
        }
        for( CompletionInfo i: completionInfo ) {
            registerCompletionInfo( i );
        }
    }

    /**
     * Register a task result for an outstanding task.
     * @param result The task result message that tells about this task.
     */
    TaskType registerTaskCompleted( TaskCompletedMessage result, long arrivalMoment )
    {
        final long id = result.taskId;    // The identifier of the task, as handed out by us.

        int ix = searchActiveTask( id );
        if( ix<0 ) {
            Globals.log.reportInternalError( "Master: ignoring reported result from task with unknown id " + id );
            return null;
        }
        ActiveTask task = activeTasks.remove( ix );
        long roundtripTime = arrivalMoment-task.startTime;
        long roundtripError = Math.abs( task.predictedDuration-roundtripTime );
	long newTransmissionTime = roundtripTime-result.workerDwellTime; // The time interval to send the task and report the result.

        if( task.deadline<arrivalMoment ) {
            //Globals.log.reportError( "Task " + task + " missed deadline by " + Service.formatNanoseconds( now-task.deadline ) );
            missedDeadlines++;
        }
        registerWorkerInfo( result.workerQueueInfo, result.completionInfo, arrivalMoment );
        task.workerTaskInfo.registerTaskCompleted( newTransmissionTime, roundtripTime, roundtripError );
        if( Settings.traceMasterProgress ){
            System.out.println(
                "Master: retired task " + task + 
                " roundtripTime=" + Service.formatNanoseconds( roundtripTime ) +
                " roundtripError=" + Service.formatNanoseconds( roundtripError ) +
                " transmissionTime=" + Service.formatNanoseconds( newTransmissionTime )
            );
        }
        return task.task.type;
    }

    /**
     * Returns true iff this worker is in a state where the master can finish.
     * @return True iff the master is allowed to finish.
     */
    boolean allowsMasterToFinish()
    {
        return activeTasks.isEmpty();
    }

    /** Register the start of a new task.
     * 
     * @param task The task that was started.
     * @param id The id given to the task.
     * @param predictedDuration The predicted duration in ns of the task.
     * @return If true, this task was added to the reservations, not
     *         to the collection of outstanding tasks.
     */
    boolean registerTaskStart( TaskInstance task, long id, long predictedDuration, long deadline )
    {
        WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( task.type );
        if( workerTaskInfo == null ) {
            System.err.println( "No worker task info for task type " + task.type );
            return true;
        }
        workerTaskInfo.incrementOutstandingTasks();
        ActiveTask j = new ActiveTask( task, id, System.nanoTime(), workerTaskInfo, predictedDuration, deadline );

        activeTasks.add( j );
        return false;
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
            System.out.println( "worker " + identifier + " (" + port + ") can handle " + type + ", local=" + local );
        }
        WorkerTaskInfo info = new WorkerTaskInfo( toString() + " task " + type, type.remainingTasks, local, pingTime );
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

    long getOptimisticRoundtripTime( TaskType taskType )
    {
        if( !enabled ) {
            if( Settings.traceTypeHandling ){
                System.out.println( "getOptimisticRoundtripTime(): worker " + identifier + " not yet enabled" );
            }
            return Long.MAX_VALUE;
        }
        WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( taskType );
        if( workerTaskInfo == null ) {
            if( Settings.traceTypeHandling ){
                System.out.println( "getOptimisticRoundtripTime(): worker " + identifier + " does not support type " + taskType );
            }
            return Long.MAX_VALUE;
        }
        return workerTaskInfo.getOptimisticRoundtripTime();
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

        if( missedDeadlines>0 ) {
            s.println( "  Missed deadlines: " + missedDeadlines );
        }
        while( keys.hasMoreElements() ){
            TaskType taskType = keys.nextElement();
            WorkerTaskInfo info = workerTaskInfoTable.get( taskType );
            if( info.didWork() ) {
                String stats = info.buildStatisticsString();
                s.println( "  " + taskType.toString() + ": " + stats );
            }
        }
    }

    /**
     * Given a print stream, print some statistics about this worker.
     * @param s The stream to print to.
     */
    private void printParsableStatistics( PrintStream s, IbisIdentifier me )
    {
        Enumeration<TaskType> keys = workerTaskInfoTable.keys();
        IbisIdentifier other = port.ibisIdentifier();

        while( keys.hasMoreElements() ){
            TaskType taskType = keys.nextElement();
            WorkerTaskInfo info = workerTaskInfoTable.get( taskType );
            if( info.didWork() ) {
                int submissions = info.getSubmissions();
                s.println( "SUBMISSIONS " + taskType.toString() + " FROM " + me + " TO " + other + " ARE " + submissions );
            }
        }
    }

    boolean isIdle( TaskType taskType )
    {
        WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( taskType );
        if( !enabled || dead || workerTaskInfo == null ) {
            return false;
        }
        return workerTaskInfo.isReady();
    }

    boolean isReady( TaskType taskType )
    {
        WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( taskType );
        if( !enabled || dead || workerTaskInfo == null ) {
            return false;
        }
        return workerTaskInfo.isReady();
    }

    void setPingStartMoment( long t )
    {
        this.pingSentTime = t;
    }

    /**
     * @return Ping time.
     */
    long getPingTime()
    {
        return pingTime;
    }

    protected void resetReservations()
    {
        Enumeration<TaskType> keys = workerTaskInfoTable.keys();

        while( keys.hasMoreElements() ){
            TaskType taskType = keys.nextElement();
            WorkerTaskInfo info = workerTaskInfoTable.get( taskType );
            info.resetReservations();
        }
    }

    /**
     * We want to run or reserve a job. Returns false if the job can be run
     * immediately; returns <code>true</code> and adds a reservation if the
     * worker is fully booked.
     * @param type The type of job we migth have to do a reservation for.
     * @return There is a reservation for this task (otherwise it can be submitted immediately).
     */
    protected boolean reserveIfNeeded( TaskType type )
    {
        WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( type );
        if( workerTaskInfo == null ) {
            System.err.println( "No worker task info for task type " + type );
            return true;
        }
        return workerTaskInfo.reserveIfNeeded();
    }

    protected long getRoundtripEstimate(TaskType type) {
        WorkerTaskInfo workerTaskInfo = workerTaskInfoTable.get( type );
        if( workerTaskInfo == null ) {
            System.err.println( "No worker task info for task type " + type );
            return Long.MAX_VALUE;
        }
	return workerTaskInfo.estimateRoundtripTime();
    }
}
