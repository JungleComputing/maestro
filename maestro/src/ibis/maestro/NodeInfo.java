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
    private final List<ActiveJob> activeJobs = new ArrayList<ActiveJob>();

    /** Info about the tasks for this particular node. */
    private final NodeJobInfo nodeJobInfoList[];

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
     *            The worker queue, which contains a WorkerQueueJobInfo class
     *            for each type.
     * @param local
     *            Is this the local node?
     */
    protected NodeInfo(IbisIdentifier ibis, WorkerQueue workerQueue,
            boolean local) {
        this.ibis = ibis;
        this.local = local;
        nodeJobInfoList = new NodeJobInfo[Globals.allJobTypes.length];
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
        for (int ix = 0; ix < Globals.allJobTypes.length; ix++) {
            final WorkerQueueJobInfo taskInfo = workerQueue.getJobInfo(ix);
            nodeJobInfoList[ix] = new NodeJobInfo(taskInfo, this,
                    estimatedPingTime);
        }
    }

    @Override
    public String toString() {
        return ibis.toString();
    }

    NodeJobInfo get(JobType t) {
        return nodeJobInfoList[t.index];
    }

    /**
     * Given a task identifier, returns the task queue entry with that id, or
     * null.
     * 
     * @param id
     *            The task identifier to search for.
     * @return The index of the ActiveJob with this id, or -1 if there isn't
     *         one.
     */
    private int searchActiveJob(long id) {
        // Note that we blindly assume that there is only one entry with
        // the given id. Reasonable because we hand out the ids ourselves,
        // and we never make mistakes...
        for (int ix = 0; ix < activeJobs.size(); ix++) {
            final ActiveJob e = activeJobs.get(ix);
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
    ArrayList<JobInstance> setDead() {
        final ArrayList<JobInstance> orphans = new ArrayList<JobInstance>();
        synchronized (this) {
            suspect = true;
            dead = true;
            for (final ActiveJob t : activeJobs) {
                orphans.add(t.job);
            }
            activeJobs.clear(); // Don't let those orphans take up memory.
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

    private synchronized ActiveJob extractActiveJob(long id) {
        final int ix = searchActiveJob(id);
        if (ix < 0) {
            return null;
        }
        return activeJobs.remove(ix);
    }

    /**
     * We failed to send the task to the destined worker, rectract it from the
     * list of active tasks.
     * 
     * @param taskId
     *            The task to retract.
     */
    void retractJob(long taskId) {
        // We ignore the result of the extract: it doesn't really matter if the
        // task was
        // in our list of not.
        extractActiveJob(taskId);
    }

    JobInstance registerJobFailed(long id) {
        final ActiveJob task = extractActiveJob(id);
        if (task == null) {
            Globals.log.reportError("Job with unknown id " + id
                    + " seems to have failed");
            return null;
        }
        task.nodeJobInfo.registerJobFailed();
        return task.job;
    }

    /**
     * Register a task result for an outstanding task.
     * 
     * @param result
     *            The task result message that tells about this task.
     * @return The task instance that was completed if it may have duplicates,
     *         or <code>null</code>
     */
    JobInstance registerJobCompleted(JobCompletedMessage result) {
        final long id = result.jobId; // The identifier of the task, as handed
        // out by us.

        final ActiveJob task = extractActiveJob(id);

        if (task == null) {
            // Not in the list of active tasks, presumably because it was
            // redundantly executed.
            return null;
        }
        final double roundtripTime = result.arrivalMoment - task.startTime;
        final NodeJobInfo nodeJobInfo = task.nodeJobInfo;
        final JobType type = task.job.type;
        if (task.getAllowanceDeadline() < result.arrivalMoment) {
            nodeJobInfo.registerMissedAllowanceDeadline();
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
            nodeJobInfo.registerMissedRescheduleDeadline();
        }
        nodeJobInfo.registerJobCompleted(roundtripTime);
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Master: retired task " + task
                    + " roundtripTime="
                    + Utils.formatSeconds(roundtripTime));
        }
        if (task.job.isOrphan()) {
            return task.job;
        }
        return null;
    }

    /**
     * Register a reception notification for a task.
     * 
     * @param result
     *            The task received message that tells about this task.
     */
    void registerJobReceived(JobReceivedMessage result) {
        final ActiveJob task;

        // The identifier of the task, as handed out by us.
        final long id = result.jobId;
        synchronized (this) {
            final int ix = searchActiveJob(id);

            if (ix < 0) {
                // Not in the list of active tasks, presumably because it was
                // redundantly executed.
                return;
            }
            task = activeJobs.get(ix);
        }
        final double transmissionTime = result.arrivalMoment - task.startTime;
        final NodeJobInfo nodeJobInfo = task.nodeJobInfo;
        if (!local) {
            // If this is not the local node, this is interesting info.
            // If it is local, we know better: transmission time is 0.
            nodeJobInfo.registerJobReceived(transmissionTime);
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
    void registerJobStart(JobInstance task, long id, double predictedDuration) {
        final JobType type = task.type;
        final NodeJobInfo workerJobInfo = nodeJobInfoList[type.index];
        if (workerJobInfo == null) {
            Globals.log
                    .reportInternalError("No worker task info for task type "
                            + type);
        } else {
            workerJobInfo.incrementOutstandingJobs();
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
        final ActiveJob j = new ActiveJob(task, id, now, workerJobInfo,
                predictedDuration, allowanceDeadline, rescheduleDeadline);
        synchronized (this) {
            activeJobs.add(j);
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

        for (final NodeJobInfo info : nodeJobInfoList) {
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
                for (final ActiveJob task : activeJobs) {
                    if (task.getAllowanceDeadline() < now) {
                        // Worker missed an allowance deadline.
                        final double t = now - task.startTime
                                + task.predictedDuration;
                        final NodeJobInfo workerJobInfo = task.nodeJobInfo;
                        if (workerJobInfo != null) {
                            workerJobInfo.updateRoundtripTimeEstimate(t);
                        }
                        task.setAllowanceDeadline(t);
                    }
                }
            }
        }
        return changed;
    }

    synchronized LocalNodeInfo getLocalInfo() {
        final int currentJobs[] = new int[nodeJobInfoList.length];
        final double transmissionTime[] = new double[nodeJobInfoList.length];
        final double predictedDuration[] = new double[nodeJobInfoList.length];

        for (int i = 0; i < nodeJobInfoList.length; i++) {
            final NodeJobInfo nodeJobInfo = nodeJobInfoList[i];
            if (nodeJobInfo == null) {
                currentJobs[i] = 0;
                transmissionTime[i] = 0;
                predictedDuration[i] = Double.POSITIVE_INFINITY;
            } else {
                currentJobs[i] = nodeJobInfo.getCurrentJobs();
                transmissionTime[i] = nodeJobInfo.getTransmissionTime();
                predictedDuration[i] = nodeJobInfo.estimateRoundtripTime();
            }
        }
        return new LocalNodeInfo(suspect, currentJobs, transmissionTime,
                predictedDuration);
    }
}
