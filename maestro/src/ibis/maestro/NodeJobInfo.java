package ibis.maestro;

import java.io.PrintStream;

/**
 * Information the node has about a particular task type on a particular node.
 */
final class NodeJobInfo {
    final WorkerQueueJobInfo taskInfo;

    private final NodeInfo nodeInfo;

    private final TimeEstimate transmissionTimeEstimate;

    private final TimeEstimate roundtripTimeEstimate;

    /** How many instances of this task does this worker currently have? */
    private int outstandingTasks = 0;

    /** How many task instances has this worker executed until now? */
    private int executedTasks = 0;

    private boolean failed = false;

    private final Counter missedAllowanceDeadlines = new Counter();

    private final Counter missedRescheduleDeadlines = new Counter();

    /**
     * Constructs a new information class for a particular task type for a
     * particular worker.
     * 
     * @param taskInfo
     *            The type of task we have administration for.
     * @param worker
     *            The worker we have administration for.
     * @param pingTime
     *            The ping time of this worker.
     */
    NodeJobInfo(WorkerQueueJobInfo taskInfo, NodeInfo worker, double pingTime) {
        this.taskInfo = taskInfo;
        this.nodeInfo = worker;

        // Totally unfounded guesses, but we should learn soon enough what the
        // real values are...
        this.transmissionTimeEstimate = new TimeEstimate(pingTime);
        this.roundtripTimeEstimate = new TimeEstimate(2 * pingTime);
        if (Settings.traceWorkerList || Settings.traceRemainingJobTime) {
            Globals.log.reportProgress("Created new WorkerTaskInfo "
                    + toString());
        }
    }

    /**
     * @return A string representation of this class instance.
     */
    @Override
    public String toString() {
        return "[taskInfo=" + taskInfo + " worker=" + nodeInfo
                + " transmissionTimeEstimate=" + transmissionTimeEstimate
                + " outstandingTasks=" + outstandingTasks + "]";
    }

    /**
     * Registers the completion of a task.
     * 
     * @param roundtripTime
     *            The total roundtrip time of this task.
     */
    synchronized void registerTaskCompleted(double roundtripTime) {
        executedTasks++;
        outstandingTasks--;
        roundtripTimeEstimate.addSample(roundtripTime);
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final String label = "task=" + taskInfo + " worker=" + nodeInfo;
            Globals.log.reportProgress(label + ": roundTripTimeEstimate="
                    + roundtripTimeEstimate);
        }
    }

    /**
     * Registers the reception of a task by the worker.
     * 
     * @param transmissionTime
     *            The transmission time of this task.
     */
    synchronized void registerTaskReceived(double transmissionTime) {
        transmissionTimeEstimate.addSample(transmissionTime);
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final String label = "task=" + taskInfo + " worker=" + nodeInfo;
            Globals.log.reportProgress(label + ": transmissionTimeEstimate="
                    + transmissionTimeEstimate);
        }
    }

    synchronized void registerTaskFailed() {
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

    /** Register that there is a new outstanding task. */
    synchronized void incrementOutstandingTasks() {
        outstandingTasks++;
    }

    /**
     * @return True iff this worker ever executed a task of this type.
     */
    private synchronized boolean didWork() {
        return (executedTasks != 0) || (outstandingTasks != 0);
    }

    synchronized double estimateRoundtripTime() {
        if (failed) {
            return Double.POSITIVE_INFINITY;
        }
        return roundtripTimeEstimate.getAverage();
    }

    synchronized void printStatistics(PrintStream s) {
        if (didWork()) {
            s.println("  " + taskInfo.type + ": executed " + executedTasks
                    + " tasks, xmit time " + transmissionTimeEstimate
                    + (failed ? " FAILED" : ""));
            final int missedAllowance = missedAllowanceDeadlines.get();
            final int missedReschedule = missedRescheduleDeadlines.get();
            if (missedAllowance > 0 || missedReschedule > 0) {
                s.println("  " + taskInfo.type
                        + ": missed deadlines: allowance: " + missedAllowance
                        + " reschedule: " + missedReschedule);
            }
        }
    }

    synchronized int getCurrentTasks() {
        return outstandingTasks;
    }

    synchronized double getTransmissionTime() {
        return transmissionTimeEstimate.getAverage();
    }

}
