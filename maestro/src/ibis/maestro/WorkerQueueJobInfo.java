package ibis.maestro;

import ibis.steel.Estimate;
import ibis.steel.Estimator;
import ibis.steel.ExponentialDecayLogEstimator;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Statistics per type for the different job types in the queue.
 * 
 * @author Kees van Reeuwijk
 */
final class WorkerQueueJobInfo {
    /** The type these statistics are about. */
    final JobType type;

    /** The workers that are willing to execute this job. */
    private final List<NodeJobInfo> workers = new LinkedList<NodeJobInfo>();

    /** The total number of jobs of this type that entered the queue. */
    private long incomingJobCount = 0;

    private int outGoingJobCount = 0;

    /** Current number of elements of this type in the queue. */
    private int elements = 0;

    private int sequenceNumber = 0;

    /** Maximal ever number of elements in the queue. */
    private int maxElements = 0;

    /** The last moment in s that the front of the queue changed. */
    private double frontChangedTime = 0;

    private boolean failed = false;

    /** The estimated time interval between jobs being dequeued. */
    private final Estimator dequeueInterval;

    private double totalWorkTime = 0.0;

    private final Estimator averageComputeTime;

    WorkerQueueJobInfo(final JobType type, final Estimate est) {
        this.type = type;
        averageComputeTime = new ExponentialDecayLogEstimator(est, 0.2);
        final double logAverage = Math.log(1 * Utils.MILLISECOND);
        final double logStdDev = Math.log(10 * Utils.MILLISECOND);
        dequeueInterval = new ExponentialDecayLogEstimator(logAverage,
                logStdDev * logStdDev, 0.2);
    }

    synchronized void printStatistics(final PrintStream s, final double workTime) {
        s.println("worker queue for " + type + ": " + incomingJobCount
                + " jobs; dequeue interval: " + dequeueInterval
                + "; maximal queue size: " + maxElements);
        final double workPercentage = 100.0 * totalWorkTime / workTime;
        final PrintStream out = s;
        if (outGoingJobCount > 0) {
            out.println("Worker: " + type + ":");
            out.printf("    # jobs           = %5d\n", outGoingJobCount);
            out.println("    total work time      = "
                    + Utils.formatSeconds(totalWorkTime)
                    + String.format(" (%.1f%%)", workPercentage));
            out.println("    work time/job        = "
                    + Utils.formatSeconds(totalWorkTime / outGoingJobCount));
            out.println("    av. dequeue interval = "
                    + dequeueInterval.getStatisticsString());
        } else {
            out.println("Worker: " + type + " is unused");
        }
    }

    int registerAdd() {
        elements++;
        if (elements > maxElements) {
            maxElements = elements;
        }
        if (elements == 1) {
            // The new entry is the front of the queue,
            // record the time it became this.
            frontChangedTime = Utils.getPreciseTime();
        }
        incomingJobCount++;
        sequenceNumber++;
        return elements;
    }

    int registerRemove() {
        final double now = Utils.getPreciseTime();
        if (frontChangedTime == 0) {
            Globals.log.reportInternalError("Variable frontChangedTime is 0??");
        } else {
            dequeueInterval.addSample(now - frontChangedTime);
        }
        elements--;
        sequenceNumber++;
        if (elements < 0) {
            Globals.log
                    .reportInternalError("Number of elements in worker queue is now negative?? type="
                            + type + " elements=" + elements);
        }
        frontChangedTime = now;
        return elements;
    }

    /**
     * Registers the completion of a job of this particular type, with the given
     * queue interval and the given work interval.
     * 
     * @param workTime
     *            The time it took to execute this job.
     * @param unpredictable
     *            If <code>true</code>, job statistics for this job type are
     *            meaningless, use the initial benchmark estimate instead.
     */
    synchronized Estimate countJob(final double workTime,
            final boolean unpredictable) {
        outGoingJobCount++;
        totalWorkTime += workTime;
        if (!unpredictable) {
            averageComputeTime.addSample(workTime);
        }
        return averageComputeTime.getEstimate();
    }

    /**
     * Registers that this node can no longer execute this type of job.
     */
    synchronized void failJob() {
        failed = true;
    }

    synchronized boolean hasFailed() {
        return failed;
    }

    void registerNode(final WorkerInfo nodeInfo) {
        final NodeJobInfo nodeJobInfo = nodeInfo.get(type);
        synchronized (this) {
            if (nodeJobInfo != null) {
                workers.add(nodeJobInfo);
            }
        }
    }

    Estimate getQueueTimePerJob() {
        return dequeueInterval.getEstimate();
    }
}