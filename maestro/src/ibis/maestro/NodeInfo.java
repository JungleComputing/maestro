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
        double estimatedPingTime = 0.0;
        if (!local) {
            estimatedPingTime = Utils.MILLISECOND
                    + Utils.MILLISECOND*Globals.rng.nextDouble();
            if (!Utils.areInSameCluster(Globals.localIbis.identifier(), ibis)) {
                // Be more pessimistic if the nodes are not in the same cluster.
                // TODO: simply look at the number of differing levels.
                estimatedPingTime *= 3;
            }
        }
        for (int ix = 0; ix < Globals.allTaskTypes.length; ix++) {
            final WorkerQueueTaskInfo taskInfo = workerQueue.getTaskInfo(ix);
            nodeTaskInfoList[ix] = new NodeTaskInfo(taskInfo, this,
                    estimatedPingTime);
        }
    }

    @Override
    public String toString() {
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
            final ActiveTask e = activeTasks.get(ix);
            if (e.id == id) {
                return ix;
            }
        }
        return -1;
    }

    /**
     * Mark this worker as dead, and return a list of active tasks of this
     * worker.
     * 
     * @return The list of task instances that were outstanding on this worker.
     */
    ArrayList<TaskInstance> setDead() {
        final ArrayList<TaskInstance> orphans = new ArrayList<TaskInstance>();
        synchronized (this) {
            suspect = true;
            dead = true;
            for (final ActiveTask t : activeTasks) {
                orphans.add(t.task);
            }
            activeTasks.clear(); // Don't let those orphans take up memory.
        }
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
    void setSuspect() {
        if (local) {
            Globals.log
                    .reportInternalError("Cannot communicate with local node "
                            + ibis + "???");
        } else {
            Globals.log.reportError("Cannot communicate with node " + ibis);
            synchronized (this) {
                suspect = true;
            }
        }
    }

    private synchronized ActiveTask extractActiveTask(long id) {
        final int ix = searchActiveTask(id);
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
        final ActiveTask task = extractActiveTask(id);
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

        final ActiveTask task = extractActiveTask(id);

        if (task == null) {
            // Not in the list of active tasks, presumably because it was
            // redundantly executed.
            return null;
        }
        final double roundtripTime = result.arrivalMoment - task.startTime;
        final NodeTaskInfo nodeTaskInfo = task.nodeTaskInfo;
        final TaskType type = task.task.type;
        if (task.getAllowanceDeadline() < result.arrivalMoment) {
            nodeTaskInfo.registerMissedAllowanceDeadline();
            if (Settings.traceMissedDeadlines) {
                Globals.log.reportProgress("Missed allowance deadline for "
                        + type
                        + " task: "
                        + " predictedDuration="
                        + Utils.formatSeconds(task.predictedDuration)
                        + " allowanceDuration="
                        + Utils.formatSeconds(task.getAllowanceDeadline()
                                - task.startTime) + " realDuration="
                        + Utils.formatSeconds(roundtripTime));
            }
        }
        if (task.rescheduleDeadline < result.arrivalMoment) {
            if (Settings.traceMissedDeadlines) {
                Globals.log.reportProgress("Missed reschedule deadline for "
                        + type
                        + " task: "
                        + " predictedDuration="
                        + Utils.formatSeconds(task.predictedDuration)
                        + " rescheduleDuration="
                        + Utils.formatSeconds(task.rescheduleDeadline
                                - task.startTime) + " realDuration="
                        + Utils.formatSeconds(roundtripTime));
            }
            nodeTaskInfo.registerMissedRescheduleDeadline();
        }
        nodeTaskInfo.registerTaskCompleted(roundtripTime);
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Master: retired task " + task
                    + " roundtripTime="
                    + Utils.formatSeconds(roundtripTime));
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
        final ActiveTask task;

        // The identifier of the task, as handed out by us.
        final long id = result.taskId;
        synchronized (this) {
            final int ix = searchActiveTask(id);

            if (ix < 0) {
                // Not in the list of active tasks, presumably because it was
                // redundantly executed.
                return;
            }
            task = activeTasks.get(ix);
        }
        final double transmissionTime = result.arrivalMoment - task.startTime;
        final NodeTaskInfo nodeTaskInfo = task.nodeTaskInfo;
        if (!local) {
            // If this is not the local node, this is interesting info.
            // If it is local, we know better: transmission time is 0.
            nodeTaskInfo.registerTaskReceived(transmissionTime);
        }
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Master: retired task " + task
                    + " transmissionTime="
                    + Utils.formatSeconds(transmissionTime));
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
     *            The predicted duration in seconds of the task.
     */
    void registerTaskStart(TaskInstance task, long id, double predictedDuration) {
        final TaskType type = task.type;
        final NodeTaskInfo workerTaskInfo = nodeTaskInfoList[type.index];
        if (workerTaskInfo == null) {
            Globals.log
                    .reportInternalError("No worker task info for task type "
                            + type);
        } else {
            workerTaskInfo.incrementOutstandingTasks();
        }
        final double now = Utils.getPreciseTime();
        final double deadlineInterval = predictedDuration
                * Settings.ALLOWANCE_DEADLINE_MARGIN;
        // Don't try to enforce a deadline interval below a certain reasonable
        // minimum.
        final double allowanceDeadline = now
                + Math.max(deadlineInterval, Settings.MINIMAL_DEADLINE);
        final double rescheduleDeadline = now + deadlineInterval
                * Settings.RESCHEDULE_DEADLINE_MULTIPLIER;
        final ActiveTask j = new ActiveTask(task, id, now, workerTaskInfo,
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

        for (final NodeTaskInfo info : nodeTaskInfoList) {
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

    boolean checkDeadlines(double now) {
        final boolean changed = false;

        if (false) {
            // TODO: enable this again when it is sane to do.
            synchronized (this) {
                for (final ActiveTask task : activeTasks) {
                    if (task.getAllowanceDeadline() < now) {
                        // Worker missed an allowance deadline.
                        final double t = now - task.startTime
                                + task.predictedDuration;
                        final NodeTaskInfo workerTaskInfo = task.nodeTaskInfo;
                        if (workerTaskInfo != null) {
                            workerTaskInfo.updateRoundtripTimeEstimate(t);
                        }
                        task.setAllowanceDeadline(t);
                    }
                }
            }
        }
        return changed;
    }

    synchronized LocalNodeInfo getLocalInfo() {
        final int currentTasks[] = new int[nodeTaskInfoList.length];
        final double transmissionTime[] = new double[nodeTaskInfoList.length];
        final double predictedDuration[] = new double[nodeTaskInfoList.length];

        for (int i = 0; i < nodeTaskInfoList.length; i++) {
            final NodeTaskInfo nodeTaskInfo = nodeTaskInfoList[i];
            if (nodeTaskInfo == null) {
                currentTasks[i] = 0;
                transmissionTime[i] = 0;
                predictedDuration[i] = Double.POSITIVE_INFINITY;
            } else {
                currentTasks[i] = nodeTaskInfo.getCurrentTasks();
                transmissionTime[i] = nodeTaskInfo.getTransmissionTime();
                predictedDuration[i] = nodeTaskInfo.estimateRoundtripTime();
            }
        }
        return new LocalNodeInfo(suspect, currentTasks, transmissionTime,
                predictedDuration);
    }
}
