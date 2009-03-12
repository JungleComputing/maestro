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
    /** The active jobs of this worker. */
    private final List<ActiveJob> activeJobs = new ArrayList<ActiveJob>();

    /** Info about the jobs for this particular node. */
    private final NodeJobInfo nodeJobInfoList[];

    private boolean suspect = false;

    private boolean dead = false; // This node is known to be dead.

    private final boolean local;

    /** The ibis this nodes lives on. */
    private final IbisIdentifier ibis;

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
            boolean local,int jobCount) {
        this.ibis = ibis;
        this.local = local;
        nodeJobInfoList = new NodeJobInfo[jobCount];
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
        for (int ix = 0; ix < jobCount; ix++) {
            final WorkerQueueJobInfo jobInfo = workerQueue.getJobInfo(ix);
            nodeJobInfoList[ix] = new NodeJobInfo(jobInfo, this,
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
     * Given a job identifier, returns the job queue entry with that id, or
     * null.
     * 
     * @param id
     *            The job identifier to search for.
     * @return The index of the ActiveJob with this id, or -1 if there isn't
     *         one.
     */
    private int searchActiveJob(long id) {
        // Note that we blindly assume that there is only one entry with
        // the given id. Reasonable, because we hand out the ids ourselves,
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
     * Mark this worker as dead, and return a list of active jobs of this
     * worker.
     * 
     * @return The list of job instances that were outstanding on this worker.
     */
    ArrayList<JobInstance> setDead() {
        final ArrayList<JobInstance> orphans = new ArrayList<JobInstance>();
        synchronized (this) {
            suspect = true;
            dead = true;
            for (final ActiveJob t : activeJobs) {
                orphans.add(t.jobInstance);
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
     * We failed to send the job to the destined worker, rectract it from the
     * list of active jobs.
     * 
     * @param jobId
     *            The job to retract.
     */
    void retractJob(long jobId) {
        // We ignore the result of the extract: it doesn't really matter if the
        // job was
        // in our list of not.
        extractActiveJob(jobId);
    }

    JobInstance registerJobFailed(long id) {
        final ActiveJob job = extractActiveJob(id);
        if (job == null) {
            Globals.log.reportError("Job with unknown id " + id
                    + " has failed");
            return null;
        }
        job.nodeJobInfo.registerJobFailed();
        return job.jobInstance;
    }

    /**
     * Register a job result for an outstanding job.
     * 
     * @param result
     *            The job result message that tells about this job.
     * @return The job instance that was completed if it may have duplicates,
     *         or <code>null</code>
     */
    JobInstance registerJobCompleted(JobList jobs,JobCompletedMessage result) {
        final long id = result.jobId; // The identifier of the job, as handed
        // out by us.

        final ActiveJob job = extractActiveJob(id);

        if (job == null) {
            // Not in the list of active jobs, presumably because it was
            // redundantly executed.
            return null;
        }
        final double roundtripTime = result.arrivalMoment - job.startTime;
        final NodeJobInfo nodeJobInfo = job.nodeJobInfo;
        final JobType stageType = job.jobInstance.getStageType(jobs);
        if (job.allowanceDeadline < result.arrivalMoment) {
            nodeJobInfo.registerMissedAllowanceDeadline();
            if (Settings.traceMissedDeadlines) {
                Globals.log.reportProgress("Missed allowance deadline for "
                        + stageType
                        + " job: " + job
                        + " predictedDuration="
                        + Utils.formatSeconds(job.predictedDuration)
                        + " allowanceDuration="
                        + Utils.formatSeconds(job.allowanceDeadline
                                - job.startTime) + " realDuration="
                                + Utils.formatSeconds(roundtripTime));
            }
        }
        if (job.rescheduleDeadline < result.arrivalMoment) {
            if (Settings.traceMissedDeadlines) {
                Globals.log.reportProgress("Missed reschedule deadline for "
                        + stageType
                        + " job: " + job
                        + " predictedDuration="
                        + Utils.formatSeconds(job.predictedDuration)
                        + " rescheduleDuration="
                        + Utils.formatSeconds(job.rescheduleDeadline
                                - job.startTime) + " realDuration="
                                + Utils.formatSeconds(roundtripTime));
            }
            nodeJobInfo.registerMissedRescheduleDeadline();
        }
        nodeJobInfo.registerJobCompleted(roundtripTime);
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Master: retired job " + job
                    + " roundtripTime="
                    + Utils.formatSeconds(roundtripTime));
        }
        if (job.jobInstance.isOrphan()) {
            return job.jobInstance;
        }
        return null;
    }

    /**
     * Register a reception notification for a job.
     * 
     * @param result
     *            The job received message that tells about this job.
     */
    void registerJobReceived(JobReceivedMessage result) {
        final ActiveJob job;

        // The identifier of the job, as handed out by us.
        final long id = result.jobId;
        synchronized (this) {
            final int ix = searchActiveJob(id);

            if (ix < 0) {
                // Not in the list of active jobs, presumably because it was
                // redundantly executed.
                return;
            }
            job = activeJobs.get(ix);
        }
        final double transmissionTime = result.arrivalMoment - job.startTime;
        final NodeJobInfo nodeJobInfo = job.nodeJobInfo;
        if (!local) {
            // If this is not the local node, this is interesting info.
            // If it is local, we know better: transmission time is 0.
            nodeJobInfo.registerJobReceived(transmissionTime);
        }
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Master: retired job " + job
                    + " transmissionTime="
                    + Utils.formatSeconds(transmissionTime));
        }
    }

    /**
     * Register the start of a new job.
     * 
     * @param jobs
     *    Information about the different types of jobs that are known.
     * @param job
     *            The job that was started.
     * @param id
     *            The id given to the job.
     * @param predictedDuration
     *            The predicted duration in seconds of the job.
     */
    void registerJobStart(JobList jobs,JobInstance job, long id, double predictedDuration) {
        final JobType stageType = job.getStageType(jobs);
        final NodeJobInfo workerJobInfo = nodeJobInfoList[stageType.index];
        if (workerJobInfo == null) {
            Globals.log
            .reportInternalError("No worker job info for job type "
                    + stageType);
        } else {
            workerJobInfo.registerJobSubmitted();
        }
        final double now = Utils.getPreciseTime();
        final double allowanceDeadlineInterval = predictedDuration
        * Settings.ALLOWANCE_DEADLINE_MARGIN;
        // Don't try to enforce a deadline interval below a certain reasonable
        // minimum.
        final double allowanceDeadline = now
        + Math.max(allowanceDeadlineInterval, Settings.MINIMAL_DEADLINE);
        final double rescheduleDeadline = now + allowanceDeadlineInterval
        * Settings.RESCHEDULE_DEADLINE_MULTIPLIER;
        final ActiveJob j = new ActiveJob(job, id, now, workerJobInfo,
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
     * Register that this node is communicating with us. If we had it suspect,
     * remove that flag. Return true iff we think this node is dead. (No, we're
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

    synchronized LocalNodeInfoList getLocalInfo() {
        final LocalNodeInfoList.LocalNodeInfo l[] = new LocalNodeInfoList.LocalNodeInfo[nodeJobInfoList.length];

        for (int i = 0; i < nodeJobInfoList.length; i++) {
            l[i] = nodeJobInfoList[i].getLocalNodeInfo();
        }
        return new LocalNodeInfoList(suspect, l);
    }
}
