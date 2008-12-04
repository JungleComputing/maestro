package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Information that the worker maintains for a master.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class NodeInfo {
    /** The active tasks of this worker. */
    private final List<ActiveTask> activeTasks = new ArrayList<ActiveTask>();

    /** Info about the tasks for this particular node. */
    private final NodeTaskInfo nodeTaskInfoList[];

    private boolean suspect = false;

    private boolean dead = false; // This node is known to be dead.

    final boolean local;

    /** The ibis this nodes lives on. */
    final IbisIdentifier ibis;

    /**
     * Constructs a new NodeInfo.
     * 
     * @param ibis
     *            The ibis identifier of the node.
     * @param workerQueue
     *            The worker queue, which contains a WorkerQueueTaskInfo class
     *            for each type.
     * @param local
     *            Is this the local node?
     */
    protected NodeInfo(IbisIdentifier ibis, WorkerQueue workerQueue,
	    boolean local) {
	this.ibis = ibis;
	this.local = local;
	nodeTaskInfoList = new NodeTaskInfo[Globals.allTaskTypes.length];
	// For non-local nodes, start with a very pessimistic ping time.
	// This means that only if we really need another node, we use it.
	// long pessimisticPingTime = local?0L:Utils.HOUR_IN_NANOSECONDS;
	long pessimisticPingTime = 0L;
	if (!local) {
	    pessimisticPingTime = Utils.MILLISECOND_IN_NANOSECONDS
		    + Globals.rng
			    .nextInt((int) Utils.MILLISECOND_IN_NANOSECONDS);
	    if (!Utils.areInSameCluster(Globals.localIbis.identifier(), ibis)) {
		// Be more pessimistic if the nodes are not in the same cluster.
		// TODO: simply look at the number of differing levels.
		pessimisticPingTime *= 3;
	    }
	}
	for (int ix = 0; ix < Globals.allTaskTypes.length; ix++) {
	    WorkerQueueTaskInfo taskInfo = workerQueue.getTaskInfo(ix);
	    boolean unpredictable = Globals.allTaskTypes[ix].unpredictable;
	    nodeTaskInfoList[ix] = new NodeTaskInfo(taskInfo, this, local,
		    unpredictable, pessimisticPingTime);
	}
    }
    
    @Override
    public String toString()
    {
        return ibis.toString();
    }

    NodeTaskInfo get(TaskType t) {
	return nodeTaskInfoList[t.index];
    }

    /**
     * Given a task identifier, returns the task queue entry with that id, or
     * null.
     * 
     * @param id
     *            The task identifier to search for.
     * @return The index of the ActiveTask with this id, or -1 if there isn't
     *         one.
     */
    private int searchActiveTask(long id) {
	// Note that we blindly assume that there is only one entry with
	// the given id. Reasonable because we hand out the ids ourselves,
	// and we never make mistakes...
	for (int ix = 0; ix < activeTasks.size(); ix++) {
	    ActiveTask e = activeTasks.get(ix);
	    if (e.id == id) {
		return ix;
	    }
	}
	return -1;
    }

    boolean registerWorkerQueueInfo(WorkerQueueInfo[] workerQueueInfo) {
	boolean changed = false;

	if (isDead()) {
	    // It is strange to get info from a dead worker, but we're not going
	    // to try and
	    // revive the worker.
	    return false;
	}
	for (int i = 0; i < workerQueueInfo.length; i++) {
	    WorkerQueueInfo workerInfo = workerQueueInfo[i];
	    NodeTaskInfo nodeTaskInfo = nodeTaskInfoList[i];

	    if (workerInfo != null && nodeTaskInfo != null) {
		// FindBug complains about the sequence number being unlocked,
		// but this is a copy...
		changed |= nodeTaskInfo.controlAllowance(
			workerInfo.queueLength,
			workerInfo.queueLengthSequenceNumber);
	    }
	}
	return changed;
    }

    /**
     * Mark this worker as dead, and return a list of active tasks of this
     * worker.
     * 
     * @return The list of task instances that were outstanding on this worker.
     */
    synchronized ArrayList<TaskInstance> setDead() {
	suspect = true;
	dead = true;
	ArrayList<TaskInstance> orphans = new ArrayList<TaskInstance>();
	for (ActiveTask t : activeTasks) {
	    orphans.add(t.task);
	}
	activeTasks.clear(); // Don't let those orphans take up memory.
	if (!orphans.isEmpty()) {
	    Globals.log.reportProgress("Rescued " + orphans.size()
		    + " orphans from dead worker " + ibis);
	}
	return orphans;
    }

    /**
     * Returns true iff this worker is suspect.
     * 
     * @return Is this worker suspect?
     */
    synchronized boolean isSuspect() {
	return suspect;
    }

    /**
     * This worker is suspect because it got a communication timeout.
     */
    synchronized void setSuspect() {
	if (local) {
	    Globals.log
		    .reportInternalError("Cannot communicate with local node "
			    + ibis + "???");
	} else {
	    Globals.log.reportError("Cannot communicate with node " + ibis);
	    suspect = true;
	}
    }

    private synchronized ActiveTask extractActiveTask(long id) {
	int ix = searchActiveTask(id);
	if (ix < 0) {
	    return null;
	}
	return activeTasks.remove(ix);
    }

    /**
     * We failed to send the task to the destined worker, rectract it from the
     * list of active tasks.
     * 
     * @param taskId
     *            The task to retract.
     */
    void retractTask(long taskId) {
	// We ignore the result of the extract: it doesn't really matter if the
	// task was
	// in our list of not.
	extractActiveTask(taskId);
    }

    TaskInstance registerTaskFailed(long id) {
	ActiveTask task = extractActiveTask(id);
	if (task == null) {
	    Globals.log.reportError("Task with unknown id " + id
		    + " seems to have failed");
	    return null;
	}
	task.nodeTaskInfo.registerTaskFailed();
	return task.task;
    }

    /**
     * Register a task result for an outstanding task.
     * 
     * @param result
     *            The task result message that tells about this task.
     * @return The task instance that was completed if it may have duplicates,
     *         or <code>null</code>
     */
    TaskInstance registerTaskCompleted(TaskCompletedMessage result) {
	final long id = result.taskId; // The identifier of the task, as handed
				       // out by us.

	ActiveTask task = extractActiveTask(id);

	if (task == null) {
	    // Not in the list of active tasks, presumably because it was
	    // redundantly executed.
	    return null;
	}
	long roundtripTime = result.arrivalMoment - task.startTime;
	NodeTaskInfo nodeTaskInfo = task.nodeTaskInfo;
	TaskType type = task.task.type;
	if (task.getAllowanceDeadline() < result.arrivalMoment) {
	    nodeTaskInfo.registerMissedAllowanceDeadline();
	    if (Settings.traceMissedDeadlines) {
		Globals.log.reportProgress("Missed allowance deadline for "
			+ type
			+ " task: "
			+ " predictedDuration="
			+ Utils.formatNanoseconds(task.predictedDuration)
			+ " allowanceDuration="
			+ Utils.formatNanoseconds(task.getAllowanceDeadline()
				- task.startTime) + " realDuration="
			+ Utils.formatNanoseconds(roundtripTime));
	    }
	}
	if (task.rescheduleDeadline < result.arrivalMoment) {
	    if (Settings.traceMissedDeadlines) {
		Globals.log.reportProgress("Missed reschedule deadline for "
			+ type
			+ " task: "
			+ " predictedDuration="
			+ Utils.formatNanoseconds(task.predictedDuration)
			+ " rescheduleDuration="
			+ Utils.formatNanoseconds(task.rescheduleDeadline
				- task.startTime) + " realDuration="
			+ Utils.formatNanoseconds(roundtripTime));
	    }
	    nodeTaskInfo.registerMissedRescheduleDeadline();
	}
	nodeTaskInfo.registerTaskCompleted(roundtripTime);
	if (Settings.traceNodeProgress) {
	    Globals.log.reportProgress("Master: retired task " + task
		    + " roundtripTime="
		    + Utils.formatNanoseconds(roundtripTime));
	}
	if (task.task.isOrphan()) {
	    return task.task;
	}
	return null;
    }

    /**
     * Register a reception notification for a task.
     * 
     * @param result
     *            The task received message that tells about this task.
     */
    void registerTaskReceived(TaskReceivedMessage result) {
	final long id = result.taskId; // The identifier of the task, as handed
				       // out by us.
	int ix = searchActiveTask(id);

	if (ix < 0) {
	    // Not in the list of active tasks, presumably because it was
	    // redundantly executed.
	    return;
	}
	ActiveTask task = activeTasks.get(ix);
	long transmissionTime = result.arrivalMoment - task.startTime;
	NodeTaskInfo nodeTaskInfo = task.nodeTaskInfo;
	nodeTaskInfo.registerTaskReceived(transmissionTime);
	if (Settings.traceNodeProgress) {
	    Globals.log.reportProgress("Master: retired task " + task
		    + " transmissionTime="
		    + Utils.formatNanoseconds(transmissionTime));
	}
    }

    /**
     * Register the start of a new task.
     * 
     * @param task
     *            The task that was started.
     * @param id
     *            The id given to the task.
     * @param predictedDuration
     *            The predicted duration in nanoseconds of the task.
     */
    void registerTaskStart(TaskInstance task, long id, long predictedDuration) {
	TaskType type = task.type;
	NodeTaskInfo workerTaskInfo = nodeTaskInfoList[type.index];
	if (workerTaskInfo == null) {
	    Globals.log
		    .reportInternalError("No worker task info for task type "
			    + type);
	} else {
	    workerTaskInfo.incrementOutstandingTasks();
	}
	long now = System.nanoTime();
	long deadlineInterval = predictedDuration
		* Settings.ALLOWANCE_DEADLINE_MARGIN;
	// Don't try to enforce a deadline interval below a certain reasonable
	// minimum.
	long allowanceDeadline = now
		+ Math.max(deadlineInterval, Settings.MINIMAL_DEADLINE);
	long rescheduleDeadline = now + deadlineInterval
		* Settings.RESCHEDULE_DEADLINE_MULTIPLIER;
	ActiveTask j = new ActiveTask(task, id, now, workerTaskInfo,
		predictedDuration, allowanceDeadline, rescheduleDeadline);
	synchronized (this) {
	    activeTasks.add(j);
	}
    }

    /**
     * Given a print stream, print some statistics about this worker.
     * 
     * @param s
     *            The stream to print to.
     */
    synchronized void printStatistics(PrintStream s) {
	s.println("Node " + ibis + (local ? " (local)" : ""));

	for (NodeTaskInfo info : nodeTaskInfoList) {
	    if (info != null) {
		info.printStatistics(s);
	    }
	}
    }

    /**
     * Returns true iff this worker is ready to do work. Specifically, if it is
     * not marked as suspect, and if it is enabled.
     * 
     * @return Whether this worker is ready to do work.
     */
    synchronized boolean isReady() {
	return !suspect;
    }

    /**
     * Register that this node is communicating with us. If we had it suspect,
     * remove that flag. Return true iff we thing this node is dead. (No we're
     * not resurrecting it.)
     * 
     * @return True iff the node is dead.
     */
    synchronized boolean registerAsCommunicating() {
	if (dead) {
	    return true;
	}
	suspect = false;
	return false;
    }

    synchronized boolean isDead() {
	return dead;
    }

    synchronized boolean checkDeadlines(long now) {
	boolean changed = false;

	for (ActiveTask task : activeTasks) {
	    if (task.getAllowanceDeadline() < now) {
		// Worker missed an allowance deadline.
		long t = now - task.startTime + task.predictedDuration;
		NodeTaskInfo workerTaskInfo = task.nodeTaskInfo;
		if (workerTaskInfo != null) {
		    workerTaskInfo.updateRoundtripTimeEstimate(t);
		}
		task.setAllowanceDeadline(t);
	    }
	}
	return changed;
    }

    synchronized LocalNodeInfo getLocalInfo() {
	int currentTasks[] = new int[nodeTaskInfoList.length];
	int allowance[] = new int[nodeTaskInfoList.length];
	long transmissionTime[] = new long[nodeTaskInfoList.length];
	long predictedDuration[] = new long[nodeTaskInfoList.length];

	for (int i = 0; i < nodeTaskInfoList.length; i++) {
	    NodeTaskInfo nodeTaskInfo = nodeTaskInfoList[i];
	    if (nodeTaskInfo == null) {
		currentTasks[i] = 0;
		transmissionTime[i] = 0;
		predictedDuration[i] = Long.MAX_VALUE;
		allowance[i] = 0;
	    } else {
		currentTasks[i] = nodeTaskInfo.getCurrentTasks();
		transmissionTime[i] = nodeTaskInfo.getTransmissionTime();
		predictedDuration[i] = nodeTaskInfo.estimateRoundtripTime();
		allowance[i] = nodeTaskInfo.getAllowance();
	    }
	}
	return new LocalNodeInfo(suspect, currentTasks, allowance,
		transmissionTime, predictedDuration);
    }

    synchronized boolean isAvailable(TaskType t) {
	return nodeTaskInfoList[t.index].isAvailable();
    }
}
