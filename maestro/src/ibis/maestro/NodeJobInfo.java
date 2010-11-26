package ibis.maestro;

import ibis.steel.Estimator;
import ibis.steel.ExponentialDecayLogEstimator;

import java.io.PrintStream;

/**
 * Information the node has about a particular job type on a particular node.
 */
final class NodeJobInfo {
    private final WorkerQueueJobInfo jobInfo;

    private final NodeInfo nodeInfo;

    private final Estimator transmissionEstimate;

    private final Estimator roundtripEstimate;

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
    NodeJobInfo(final WorkerQueueJobInfo jobInfo, final NodeInfo worker,
            final double pingTime) {
        this.jobInfo = jobInfo;
        nodeInfo = worker;

        // Totally unfounded guesses, but we should learn soon enough what the
        // real values are...
        transmissionEstimate = new ExponentialDecayLogEstimator(pingTime,
                pingTime);
        roundtripEstimate = new ExponentialDecayLogEstimator(2 * pingTime,
                2 * pingTime);
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
                + " transmissionEstimate=" + transmissionEstimate
                + " outstandingJobs=" + outstandingJobs + "]";
    }

    /**
     * Registers the completion of a job.
     * 
     * @param roundtripTime
     *            The total round-trip time of this job.
     */
    synchronized void registerJobCompleted(final double roundtripTime) {
        executedJobs++;
        outstandingJobs--;
        roundtripEstimate.addSample(roundtripTime);
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final String label = "job=" + jobInfo + " worker=" + nodeInfo;
            Globals.log.reportProgress(label + ": roundTripEstimate="
                    + roundtripEstimate);
        }
    }

    /**
     * Registers the reception of a job by the worker.
     * 
     * @param transmissionTime
     *            The transmission time of this job.
     */
    synchronized void registerJobReceived(final double transmissionTime) {
        transmissionEstimate.addSample(transmissionTime);
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final String label = "job=" + jobInfo + " worker=" + nodeInfo;
            Globals.log.reportProgress(label + ": transmissionEstimate="
                    + transmissionEstimate);
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

    /** Register that there is a new outstanding job. */
    synchronized void registerJobSubmitted() {
        outstandingJobs++;
    }

    synchronized void printStatistics(final PrintStream s) {
        if (executedJobs != 0 || outstandingJobs != 0) {
            s.println("  " + jobInfo.type + ": executed " + executedJobs
                    + " jobs, xmit time " + transmissionEstimate
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

    synchronized LocalNodeInfo getLocalNodeInfo() {
        final Estimator transmissionTime = transmissionEstimate.getEstimate();
        Estimator predictedDuration;
        if (failed) {
            predictedDuration = null;
        } else {
            predictedDuration = roundtripEstimate.getEstimate();
        }
        return new LocalNodeInfo(outstandingJobs, transmissionTime,
                predictedDuration);
    }
}
