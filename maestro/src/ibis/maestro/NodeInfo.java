package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;

/**
 * Information that the worker maintains for a master.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class NodeInfo
{
    /** Our local identifier of this node. */
    final NodeIdentifier ourIdentifierForNode;

    /** The identifier the node wants to see when we talk to it. */
    private NodeIdentifier theirIdentifierForUs;

    /** The active tasks of this worker. */
    private final ArrayList<ActiveTask> activeTasks = new ArrayList<ActiveTask>();

    /** Info about the tasks for this particular node. */
    private final ArrayList<NodeTaskInfo> nodeTaskInfoList = new ArrayList<NodeTaskInfo>();

    private boolean suspect = true;   // A node starts as suspect.

    private boolean dead = false;     // This node is known to be dead.

    final boolean local;

    /** The duration of the ping round-trip for this worker. */
    private long pingTime;

    private int missedAllowanceDeadlines = 0;
    private int missedRescheduleDeadlines = 0;

    /** The ibis this nodes lives on. */
    final IbisIdentifier ibis;

    /**
     * Constructs a new NodeInfo.
     * @param id The identifier of the node.
     * @param ibis The ibis identifier of the node.
     * @param local Is this the local node?
     */
    protected NodeInfo( NodeIdentifier id, IbisIdentifier ibis, boolean local )
    {
        this.ourIdentifierForNode = id;
        this.ibis = ibis;
        this.local = local;
        // For non-local nodes, start with a very pessimistic ping time.
        // This means that if we really need another node, we use it, otherwise
        // we wait for the measurement of the real ping time.
        pingTime = local?0L:Service.HOUR_IN_NANOSECONDS;
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
        NodeTaskInfo workerTaskInfo = nodeTaskInfoList.get( completionInfo.type.index );

        if( workerTaskInfo == null ) {
            return;
        }
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Master: worker " + ourIdentifierForNode + ":" + completionInfo );
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
        NodeTaskInfo workerTaskInfo = nodeTaskInfoList.get( info.type.index );

        if( workerTaskInfo == null ) {
            return;
        }
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Master: worker " + ourIdentifierForNode + ":" + info );
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
    private static void setPingTime( ArrayList<NodeTaskInfo> l, long pingTime )
    {
        for( NodeTaskInfo wi: l ) {
            if( wi != null ) {
                wi.setPingTime( pingTime );
            }
        }        
    }

    /**
     * Sets the identifier this node uses for us to the given value.
     * @param id The identifier on this master.
     */
    synchronized void setTheirIdentifierForUs( NodeIdentifier id )
    {
        this.theirIdentifierForUs = id;
    }
    
    /**
     * Gets the identifier that we have on this node.
     * @return The identifier.
     */
    synchronized NodeIdentifier getTheirIdentifierForUs()
    {
        return theirIdentifierForUs;
    }

    /** Mark this worker as dead, and return a list of active tasks
     * of this worker.
     * @return The list of task instances that were outstanding on this worker.
     */
    synchronized ArrayList<TaskInstance> setDead()
    {
        suspect = true;
        dead = true;
        ArrayList<TaskInstance> orphans = new ArrayList<TaskInstance>();
        for( ActiveTask t: activeTasks ) {
            orphans.add( t.task );
        }
        activeTasks.clear(); // Don't let those orphans take up memory.
        if( !orphans.isEmpty() ) {
            System.out.println( "Rescued " + orphans.size() + " orphans from dead worker " + ourIdentifierForNode );
        }
        return orphans;
    }

    /**
     * Returns true iff this worker is suspect.
     * @return Is this worker suspect?
     */
    synchronized boolean isSuspect()
    {
        return suspect;
    }

    /**
     * This worker is suspect because it got a communication timeout.
     */
    synchronized void setSuspect()
    {
        if( local ) {
            System.out.println( "Cannot communicate with local node " + ourIdentifierForNode + "???" );
        }
        else {
            System.out.println( "Cannot communicate with node " + ourIdentifierForNode );
            suspect = true;
        }
    }
    
    synchronized void setPingTime( long t )
    {
	pingTime = t;
        setPingTime( nodeTaskInfoList, pingTime );
        if( !dead ) {
            // We seem to be communicating.
            suspect = false;
        }
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Ping time to node " + ourIdentifierForNode + " is " + Service.formatNanoseconds( pingTime ) );
        }	
    }

    synchronized void registerWorkerInfo( WorkerQueueInfo[] workerQueueInfo, CompletionInfo[] completionInfo )
    {
        if( dead ) {
            // It is strange to get info from a dead worker, but we're not going to try and
            // revive the worker.
            return;
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
            Globals.log.reportProgress( "Master: got new information from " + ourIdentifierForNode + ": " +  s );
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
    synchronized TaskType registerTaskCompleted( TaskCompletedMessage result )
    {
        final long id = result.taskId;    // The identifier of the task, as handed out by us.

        int ix = searchActiveTask( id );
        if( ix<0 ) {
            Globals.log.reportInternalError( "Master: ignoring reported result from task with unknown id " + id );
            return null;
        }
        ActiveTask task = activeTasks.remove( ix );
        long roundtripTime = result.arrivalMoment-task.startTime;
        long roundtripError = Math.abs( task.predictedDuration-roundtripTime );
        long newTransmissionTime = roundtripTime-result.workerDwellTime; // The time interval to send the task and report the result.

        if( task.allowanceDeadline<result.arrivalMoment ) {
            missedAllowanceDeadlines++;
            if( Settings.traceMissedDeadlines ){
                Globals.log.reportProgress(
                    "Missed allowance deadline for " + task.task.type + " task: "
                    + " predictedDuration=" + Service.formatNanoseconds( task.predictedDuration )
                    + " allowanceDuration=" + Service.formatNanoseconds( task.allowanceDeadline-task.startTime )
                    + " realDuration=" + Service.formatNanoseconds( roundtripTime )
                );
            }
        }
        if( task.rescheduleDeadline<result.arrivalMoment ) {
            if( Settings.traceMissedDeadlines ){
                Globals.log.reportProgress(
                    "Missed reschedule deadline for " + task.task.type + " task: "
                    + " predictedDuration=" + Service.formatNanoseconds( task.predictedDuration )
                    + " rescheduleDuration=" + Service.formatNanoseconds( task.rescheduleDeadline-task.startTime )
                    + " realDuration=" + Service.formatNanoseconds( roundtripTime )
                );
            }
            missedRescheduleDeadlines++;
        }
        registerWorkerInfo( result.workerQueueInfo, result.completionInfo );
        task.workerTaskInfo.registerTaskCompleted( newTransmissionTime, roundtripTime, roundtripError );
        if( Settings.traceNodeProgress ){
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
    synchronized boolean allowMasterToFinish()
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
    synchronized boolean registerTaskStart( TaskInstance task, long id, long predictedDuration )
    {
        NodeTaskInfo workerTaskInfo = nodeTaskInfoList.get( task.type.index );
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
    synchronized void retractTask( long id )
    {
        int ix = searchActiveTask( id );
        if( ix<0 ) {
            Globals.log.reportInternalError( "ignoring task retraction for unknown id " + id );
            return;
        }
        ActiveTask task = activeTasks.remove( ix );
        System.out.println( "Master: retracted task " + task );
    }

    /**
     * @param taskInfo The task type to register for.
     */
    synchronized NodeTaskInfo registerTaskType( TaskInfo taskInfo )
    {
        int ix = taskInfo.type.index;
        while( ix+1>nodeTaskInfoList.size() ) {
            nodeTaskInfoList.add( null );
        }
        NodeTaskInfo info = nodeTaskInfoList.get( ix );
        if( info == null ) {
            // This is new information.
            info = new NodeTaskInfo( taskInfo, this, local, pingTime );
            nodeTaskInfoList.set( ix, info );
            if( Settings.traceTypeHandling ){
                System.out.println( "node " + ourIdentifierForNode + " can handle " + taskInfo + ", local=" + local );
            }
        }
        return info;
    }
    
    synchronized NodeTaskInfo getNodeTaskInfo( TaskType taskType )
    {
        return nodeTaskInfoList.get( taskType.index );
    }

    /**
     * Given a print stream, print some statistics about this worker.
     * @param s The stream to print to.
     */
    synchronized void printStatistics( PrintStream s )
    {
        s.println( "Node " + ourIdentifierForNode + (local?" (local)":"") );

        if( missedAllowanceDeadlines>0 ) {
            int total = 0;
            for( NodeTaskInfo wti: nodeTaskInfoList ){
                if( wti != null ){
                    total += wti.getSubmissions();
                }
            }
            s.println( "  Missed deadlines: allowance: " + missedAllowanceDeadlines  + " reschedule: " + missedRescheduleDeadlines + " of " + total );
        }
        for( NodeTaskInfo info: nodeTaskInfoList ) {
            if( info != null ) {
                info.printStatistics( s );
            }
        }
    }

    protected void resetReservations()
    {
        for( NodeTaskInfo info: nodeTaskInfoList ) {
            if( info != null ) {
                info.resetReservations();
            }
        }
    }

    /**
     * Returns true iff this worker is ready to do work. Specifically, if it is not marked
     * as suspect, and if it is enabled.
     * @return Whether this worker is ready to do work.
     */
    synchronized boolean isReady()
    {
        return !suspect && pingTime != Long.MAX_VALUE && theirIdentifierForUs != null;
    }

    /** 
     * Register that this node is communicating with us. If we had it
     * suspect, remove that flag.
     * Return true iff we thing this node is dead. (No we're not resurrecting it.)
     * @return True iff the node is dead.
     */
    synchronized boolean registerAsCommunicating()
    {
        if( dead ) {
            return true;
        }
        suspect = false;
        return false;
    }

    synchronized boolean isDead()
    {
        return dead;
    }
}
