package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;

/**
 * Information about a worker in the list of a master.
 */
final class WorkerInfo {
    /** Our identifier with this worker. */
    final MasterIdentifier identifierWithWorker;

    /** The active tasks of this worker. */
    private final ArrayList<ActiveTask> activeTasks = new ArrayList<ActiveTask>();

    private final ArrayList<WorkerTaskInfo> workerTaskInfoList = new ArrayList<WorkerTaskInfo>();

    /** Our local identifier of this worker. */
    final Master.WorkerIdentifier identifier;

    /** The receive port of the worker. */
    final ReceivePortIdentifier port;

    /** Set to true when we know the worker is ready to talk to us. */
    private boolean enabled = false;

    final boolean local;

    final int workerThreads;

    private boolean suspect = false;  // This worker <em>may</em> be dead.

    private boolean dead = false;     // This worker is known to be dead.

    /** The time the accept message was sent to this worker.
     * The roundtrip time determines the ping duration of this worker.
     */
    private long pingSentTime = 0;

    /** The duration of the ping round-trip for this worker. */
    private long pingTime;

    private int missedAllowanceDeadlines = 0;
    private int missedRescheduleDeadlines = 0;

    WorkerInfo( WorkerList wl, ReceivePortIdentifier port, WorkerIdentifier identifier, MasterIdentifier identifierForWorker, boolean local, int workerThreads, TaskType[] types )
    {
        this.port = port;
        this.identifier = identifier;
        this.identifierWithWorker = identifierForWorker;
        this.local = local;
        this.workerThreads = workerThreads;
        // Initial, totally unfounded, guess for the ping time.
        this.pingTime = local?Service.MICROSECOND_IN_NANOSECONDS:Service.MILLISECOND_IN_NANOSECONDS;
        for( TaskType t: types ) {
            registerTaskType( wl.getTaskInfo( t ) );
        }
    }

    /**
     * Returns a string representation of this worker info. (Overrides method in superclass.)
     * @return The worker info.
     */
    @Override
    public String toString()
    {
        return identifier.toString();
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
        WorkerTaskInfo workerTaskInfo = workerTaskInfoList.get( completionInfo.type.index );

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
        WorkerTaskInfo workerTaskInfo = workerTaskInfoList.get( info.type.index );

        if( workerTaskInfo == null ) {
            return;
        }
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Master: worker " + identifier + ":" + info );
        }
        workerTaskInfo.setDequeueTime( info.dequeueTime );
        workerTaskInfo.setComputeTime( info.computeTime );
        workerTaskInfo.controlAllowance( info.queueLength );
    }

    /** Update all task info to take into account the given ping time. If somewhere the initial
     * estimate is used, at least this is a slightly more accurate one.
     * @param l The task info table to update.
     * @param pingTime The ping time to this worker.
     */
    private static void setPingTime( ArrayList<WorkerTaskInfo> l, long pingTime )
    {
        for( WorkerTaskInfo wi: l ) {
            if( wi != null ) {
                wi.setPingTime( pingTime );
            }
        }        
    }

    void registerWorkerInfo( WorkerQueueInfo[] workerQueueInfo, CompletionInfo[] completionInfo, long arrivalMoment )
    {
        if( dead ) {
            // It is strange to get info from a dead worker, but we're not going to try and
            // revive the worker.
            return;
        }
        enabled = true;   // The worker now has its administration in order. We can submit jobs.
        if( pingSentTime != 0 ) {
            // We are measuring this round-trip time.
            pingTime = arrivalMoment-pingSentTime;
            pingSentTime = 0L;  // We're no longer measuring a ping time.
            setPingTime( workerTaskInfoList, pingTime );
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportProgress( "Master: ping time to worker " + identifier + " is " + Service.formatNanoseconds( pingTime ) );
            }
        }
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
     * @param arrivalMoment The time in ns the message arrived.
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

        if( task.allowanceDeadline<arrivalMoment ) {
            missedAllowanceDeadlines++;
        }
        if( task.rescheduleDeadline<arrivalMoment ) {
            if( Settings.traceMissedDeadlines ){
                Globals.log.reportProgress(
                    "Missed allowance deadline for " + task.task.type + " task: "
                    + " predictedDuration=" + Service.formatNanoseconds( task.predictedDuration )
                    + " allowanceDuration=" + Service.formatNanoseconds( task.allowanceDeadline-task.startTime )
                    + " rescheduleDuration=" + Service.formatNanoseconds( task.rescheduleDeadline-task.startTime )
                    + " realDuration=" + Service.formatNanoseconds( roundtripTime )
                );
            }
            missedRescheduleDeadlines++;
        }
        registerWorkerInfo( result.workerQueueInfo, result.completionInfo, arrivalMoment );
        task.workerTaskInfo.registerTaskCompleted( newTransmissionTime, roundtripTime, roundtripError );
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress(
                "Master: retired task " + task
                + " roundtripTime=" + Service.formatNanoseconds( roundtripTime )
                + " roundtripError=" + Service.formatNanoseconds( roundtripError )
                + " transmissionTime=" + Service.formatNanoseconds( newTransmissionTime )
            );
        }
        return task.task.type;
    }

    /**
     * Returns true iff this worker is in a state where the master can finish.
     * @return True iff the master is allowed to finish.
     */
    boolean allowMasterToFinish()
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
    boolean registerTaskStart( TaskInstance task, long id, long predictedDuration )
    {
        WorkerTaskInfo workerTaskInfo = workerTaskInfoList.get( task.type.index );
        if( workerTaskInfo == null ) {
            System.err.println( "No worker task info for task type " + task.type );
            return true;
        }
        workerTaskInfo.incrementOutstandingTasks();
        long now = System.nanoTime();
        long allowanceDeadline = now + predictedDuration*Settings.ALLOWANCE_DEADLINE_MARGIN;
        long rescheduleDeadline = now + predictedDuration*Settings.RESCHEDULE_DEADLINE_MARGIN;
        ActiveTask j = new ActiveTask( task, id, now, workerTaskInfo, predictedDuration, allowanceDeadline, rescheduleDeadline );
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
     * @param taskInfoOnMaster The task type to register for.
     */
    private void registerTaskType( TaskInfoOnMaster taskInfoOnMaster )
    {
        if( Settings.traceTypeHandling ){
            System.out.println( "worker " + identifier + " (" + port + ") can handle " + taskInfoOnMaster + ", local=" + local );
        }
        int ix = taskInfoOnMaster.type.index;
        while( ix+1>workerTaskInfoList.size() ) {
            workerTaskInfoList.add( null );
        }
        WorkerTaskInfo info = new WorkerTaskInfo( taskInfoOnMaster, this, local, workerThreads, pingTime );
        workerTaskInfoList.set( ix, info );
    }
    
    WorkerTaskInfo getTaskInfo( TaskType taskType )
    {
        return workerTaskInfoList.get( taskType.index );
    }

    /**
     * Returns true iff this worker is dead.
     * @return Is this worker dead?
     */
    boolean isDead()
    {
        return dead;
    }
    
    /**
     * Returns true iff this worker is suspect.
     * @return Is this worker suspect?
     */
    boolean isSuspect()
    {
	return suspect;
    }

    /** Mark this worker as dead, and return a list of active tasks
     * of this worker.
     * @return The list of task instances that were outstanding on this worker.
     */
    ArrayList<TaskInstance> setDead()
    {
	suspect = true;
        dead = true;
        ArrayList<TaskInstance> orphans = new ArrayList<TaskInstance>();
        for( ActiveTask t: activeTasks ) {
            orphans.add( t.task );
        }
        activeTasks.clear(); // Don't let those orphans take up memory.
        if( !orphans.isEmpty() ) {
            System.out.println( "Rescued " + orphans.size() + " orphans from dead worker " + identifier );
        }
        return orphans;
    }

    /**
     * Given a print stream, print some statistics about this worker.
     * @param s The stream to print to.
     */
    void printStatistics( PrintStream s )
    {
        s.println( "Worker " + identifier + (local?" (local)":"") );

        if( missedAllowanceDeadlines>0 ) {
            s.println( "  Missed deadlines: " + missedAllowanceDeadlines );
        }
        for( WorkerTaskInfo info: workerTaskInfoList ) {
            if( info != null && info.didWork() ) {
                String stats = info.buildStatisticsString();
                s.println( "  " + info + ": " + stats );
            }
        }
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
        for( WorkerTaskInfo info: workerTaskInfoList ) {
            if( info != null ) {
                info.resetReservations();
            }
        }
    }

    /**
     * This worker is suspect because it got a communication timeout.
     */
    protected void setSuspect()
    {
        if( local ) {
            System.out.println( "Cannot communicate with local worker " + identifier + "???" );
        }
        else {
            System.out.println( "Cannot communicate with worker " + identifier );
            suspect = true;
        }
    }

    /**
     * This worker is no longer suspect.
     */
    protected void setUnsuspect()
    {
        if( suspect && !dead ) {
            System.out.println( "Restored contact with worker " + identifier );
            suspect = false;
        }
    }

    /**
     * This worker is no longer suspect.
     * @param node The node to report any change of state to.
     */
    protected void setUnsuspect( Node node )
    {
        if( suspect && !dead ) {
            System.out.println( "Restored contact with worker " + identifier );
            suspect = false;
            node.setUnsuspectOnWorker( port.ibisIdentifier() );
        }
    }

    /**
     * Returns true iff this worker is ready to do work. Specifically, if it is not marked
     * as suspect, and if it is enabled.
     * @return Whether this worker is ready to do work.
     */
    boolean isReady()
    {
        return !suspect && enabled;
    }
}
