package ibis.maestro;

import java.io.PrintStream;

/**
 * Information the node has about a particular job type on a particular node.
 */
final class NodeJobInfo {
    private final WorkerQueueJobInfo jobInfo;

    private final NodeInfo nodeInfo;

    private final TimeEstimate transmissionTimeEstimate;

    private final TimeEstimate roundtripTimeEstimate;

    /** How many instances of this job does this worker currently have? */
    private int outstandingJobs = 0;

    /** How many job instances has this worker executed until now? */
    private int executedJobs = 0;

    private boolean failed = false;

    private final Counter missedAllowanceDeadlines = new Counter();

    private final Counter missedRescheduleDeadlines = new Counter();

    /**
     * Constructs a new information class for a particular job type for a
     * particular worker.
     * 
     * @param jobInfo
     *            The type of job we have administration for.
     * @param worker
     *            The worker we have administration for.
     * @param pingTime
     *            The ping time of this worker.
     */
    NodeJobInfo(WorkerQueueJobInfo jobInfo, NodeInfo worker, double pingTime) {
        this.jobInfo = jobInfo;
        this.nodeInfo = worker;

        // Totally unfounded guesses, but we should learn soon enough what the
        // real values are...
        this.transmissionTimeEstimate = new TimeEstimate(pingTime);
        this.roundtripTimeEstimate = new TimeEstimate(2 * pingTime);
        if (Settings.traceWorkerList || Settings.traceRemainingJobTime) {
            Globals.log.reportProgress("Created new WorkerJobInfo "
                    + toString());
        }
    }

    /**
     * @return A string representation of this class instance.
     */
    @Override
    public String toString() {
        return "[jobInfo=" + jobInfo + " worker=" + nodeInfo
                + " transmissionTimeEstimate=" + transmissionTimeEstimate
                + " outstandingJobs=" + outstandingJobs + "]";
    }

    /**
     * Registers the completion of a job.
     * 
     * @param roundtripTime
     *            The total roundtrip time of this job.
     */
    synchronized void registerJobCompleted(double roundtripTime) {
        executedJobs++;
        outstandingJobs--;
        roundtripTimeEstimate.addSample(roundtripTime);
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final String label = "job=" + jobInfo + " worker=" + nodeInfo;
            Globals.log.reportProgress(label + ": roundTripTimeEstimate="
                    + roundtripTimeEstimate);
        }
    }

    /**
     * Registers the reception of a job by the worker.
     * 
     * @param transmissionTime
     *            The transmission time of this job.
     */
    synchronized void registerJobReceived(double transmissionTime) {
        transmissionTimeEstimate.addSample(transmissionTime);
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final String label = "job=" + jobInfo + " worker=" + nodeInfo;
            Globals.log.reportProgress(label + ": transmissionTimeEstimate="
                    + transmissionTimeEstimate);
        }
    }

    synchronized void registerJobFailed() {
        failed = true;
    }

    void registerMissedAllowanceDeadline() {
        missedAllowanceDeadlines.add();
    }

    void registerMissedRescheduleDeadline() {
        missedRescheduleDeadlines.add();
    }

    /**
     * Update the roundtrip time estimate with the given value. (Used by the
     * handling of missed deadlines.
     * 
     * @param t
     *            The new estimate of the roundtrip time.
     */
    synchronized void updateRoundtripTimeEstimate(double t) {
        roundtripTimeEstimate.addSample(t);
    }

    /** Register that there is a new outstanding job. */
    synchronized void incrementOutstandingJobs() {
        outstandingJobs++;
    }

    /**
     * @return True iff this worker ever executed a job of this type.
     */
    private synchronized boolean didWork() {
        return (executedJobs != 0) || (outstandingJobs != 0);
    }

    synchronized double estimateRoundtripTime() {
        if (failed) {
            return Double.POSITIVE_INFINITY;
        }
        return roundtripTimeEstimate.getAverage();
    }

    synchronized void printStatistics(PrintStream s) {
        if (didWork()) {
            s.println("  " + jobInfo.type + ": executed " + executedJobs
                    + " jobs, xmit time " + transmissionTimeEstimate
                    + (failed ? " FAILED" : ""));
            final int missedAllowance = missedAllowanceDeadlines.get();
            final int missedReschedule = missedRescheduleDeadlines.get();
            if (missedAllowance > 0 || missedReschedule > 0) {
                s.println("  " + jobInfo.type
                        + ": missed deadlines: allowance: " + missedAllowance
                        + " reschedule: " + missedReschedule);
            }
        }
    }

    synchronized int getCurrentJobs() {
        return outstandingJobs;
    }

    synchronized double getTransmissionTime() {
        return transmissionTimeEstimate.getAverage();
    }

}
