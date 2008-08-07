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
    /** The active tasks of this worker. */
    private final ArrayList<ActiveTask> activeTasks = new ArrayList<ActiveTask>();

    /** Info about the tasks for this particular node. */
    private final NodeTaskInfo nodeTaskInfoList[];

    private boolean suspect = false;

    private boolean dead = false;     // This node is known to be dead.

    final boolean local;

    private int missedAllowanceDeadlines = 0;
    private int missedRescheduleDeadlines = 0;

    /** The ibis this nodes lives on. */
    final IbisIdentifier ibis;

    /**
     * Constructs a new NodeInfo.
     * @param ibis The ibis identifier of the node.
     * @param local Is this the local node?
     */
    protected NodeInfo( IbisIdentifier ibis, TaskInfoList taskInfoList, boolean local )
    {
        this.ibis = ibis;
        this.local = local;
        nodeTaskInfoList = new NodeTaskInfo[Globals.numberOfTaskTypes];
        // For non-local nodes, start with a very pessimistic ping time.
        // This means that if we really need another node, we use it, otherwise
        // we wait for the measurement of the real ping time.
        long pessimisticPingTime = local?0L:Service.HOUR_IN_NANOSECONDS;
        for( TaskType type: Globals.supportedTaskTypes ) {
            int ix = type.index;
            TaskInfo taskInfo = taskInfoList.getTaskInfo( type );
            nodeTaskInfoList[ix] = new NodeTaskInfo( taskInfo, this, local, pessimisticPingTime );
        }
        taskInfoList.registerNode( this );
    }
    
    NodeTaskInfo get( TaskType t )
    {
        return nodeTaskInfoList[t.index];
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

    private void registerWorkerQueueInfo( WorkerQueueInfo info )
    {
        if( info == null ) {
            return;
        }

        NodeTaskInfo nodeTaskInfo = nodeTaskInfoList[info.type.index];

        if( nodeTaskInfo == null ) {
            return;
        }
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Master: worker " + ibis + ":" + info );
        }
        nodeTaskInfo.setWorkerQueueInfo( info );
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
            System.out.println( "Rescued " + orphans.size() + " orphans from dead worker " + ibis );
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
            System.out.println( "Cannot communicate with local node " + ibis + "???" );
        }
        else {
            System.out.println( "Cannot communicate with node " + ibis );
            suspect = true;
        }
    }

    void registerNodeInfo( WorkerQueueInfo[] workerQueueInfo )
    {
        if( dead ) {
            // It is strange to get info from a dead worker, but we're not going to try and
            // revive the worker.
            return;
        }
        for( WorkerQueueInfo i: workerQueueInfo ) {
            registerWorkerQueueInfo( i );
        }
    }
    
    private synchronized ActiveTask extractActiveTask( long id )
    {
        int ix = searchActiveTask( id );
        if( ix<0 ) {
            Globals.log.reportInternalError( "Ignoring reported result from task with unknown id " + id );
            return null;
        }
        return activeTasks.remove( ix );
    }

    /**
     * Register a task result for an outstanding task.
     * @param result The task result message that tells about this task.
     * @param arrivalMoment The time in ns the message arrived.
     */
    TaskType registerTaskCompleted( TaskCompletedMessage result )
    {
        final long id = result.taskId;    // The identifier of the task, as handed out by us.

        ActiveTask task = extractActiveTask( id );
        long roundtripTime = result.arrivalMoment-task.startTime;
        long roundtripError = Math.abs( task.predictedDuration-roundtripTime );
        long newTransmissionTime = roundtripTime-result.workerDwellTime; // The time interval to send the task and report the result.

        if( task.getAllowanceDeadline()<result.arrivalMoment ) {
            missedAllowanceDeadlines++; // TODO: locked
            if( Settings.traceMissedDeadlines ){
                Globals.log.reportProgress(
                    "Missed allowance deadline for " + task.task.type + " task: "
                    + " predictedDuration=" + Service.formatNanoseconds( task.predictedDuration )
                    + " allowanceDuration=" + Service.formatNanoseconds( task.getAllowanceDeadline()-task.startTime )
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
            missedRescheduleDeadlines++;  // TODO: locked
        }
        registerNodeInfo( result.workerQueueInfo );
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

    /** Register the start of a new task.
     * 
     * @param task The task that was started.
     * @param id The id given to the task.
     * @param predictedDuration The predicted duration in ns of the task.
     */
    void registerTaskStart( TaskInstance task, long id, long predictedDuration )
    {
        NodeTaskInfo workerTaskInfo = nodeTaskInfoList[task.type.index];
        if( workerTaskInfo == null ) {
            System.err.println( "No worker task info for task type " + task.type );
        }
        else {
            workerTaskInfo.incrementOutstandingTasks();
        }
        long now = System.nanoTime();
        long allowanceDeadline = now + predictedDuration*Settings.ALLOWANCE_DEADLINE_MARGIN;
        long rescheduleDeadline = now + predictedDuration*Settings.RESCHEDULE_DEADLINE_MARGIN;
        ActiveTask j = new ActiveTask( task, id, now, workerTaskInfo, predictedDuration, allowanceDeadline, rescheduleDeadline );
        synchronized( this ) {
            activeTasks.add( j );
        }
    }

    /** Given a task id, retract it from the administration.
     * For some reason we could not send this task to the worker.
     * @param id The identifier of the task.
     */
    void retractTask( long id )
    {
	ActiveTask task = extractActiveTask( id );
        if( task == null ) {
            Globals.log.reportInternalError( "ignoring task retraction for unknown id " + id );
            return;
        }
        System.out.println( "Master: retracted task " + task );
    }

    /**
     * Given a print stream, print some statistics about this worker.
     * @param s The stream to print to.
     */
    synchronized void printStatistics( PrintStream s )
    {
        s.println( "Node " + ibis + (local?" (local)":"") );

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

    /**
     * Returns true iff this worker is ready to do work. Specifically, if it is not marked
     * as suspect, and if it is enabled.
     * @return Whether this worker is ready to do work.
     */
    synchronized boolean isReady()
    {
        return !suspect;
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

    synchronized void checkDeadlines( long now )
    {
        for( ActiveTask task: activeTasks ) {
            if( task.getAllowanceDeadline()<now ) {
                // Worker missed an allowance deadline.
                long t = now-task.startTime+task.predictedDuration;
                task.workerTaskInfo.updateRoundtripTimeEstimate( t );
                task.setAllowanceDeadline( t );
            }
        }
    }

    /** FIXME.
     * @return
     */
    synchronized LocalNodeInfo getLocalInfo()
    {
        int currentTasks[] = new int[nodeTaskInfoList.length];
        int allowance[] = new int[nodeTaskInfoList.length];
        long transmissionTime[] = new long[nodeTaskInfoList.length];
        long predictedDuration[] = new long[nodeTaskInfoList.length];

        for( int i=0; i<nodeTaskInfoList.length; i++ ) {
            NodeTaskInfo nodeTaskInfo = nodeTaskInfoList[i];
            if( nodeTaskInfo == null ) {
                currentTasks[i] = 0;
                transmissionTime[i] = 0;
                predictedDuration[i] = Long.MAX_VALUE;
                allowance[i] = 0;
            }
            else {
                currentTasks[i] = nodeTaskInfo.getCurrentTasks();
                transmissionTime[i] = nodeTaskInfo.getTransmissionTime();
                predictedDuration[i] = nodeTaskInfo.getPredictedDuration();
                allowance[i] = nodeTaskInfo.getAllowance();
            }
        }
        return new LocalNodeInfo( suspect, currentTasks, allowance, transmissionTime, predictedDuration );
    }
}
