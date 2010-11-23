package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.steel.ConstantEstimator;
import ibis.steel.Estimator;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * A packet of node update info.
 * 
 * @author Kees van Reeuwijk.
 */
class NodePerformanceInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The node this information is for. */
    final IbisIdentifier source;

    /**
     * For each type of job we know, the estimated time it will take to complete
     * the remaining jobs of this job.
     */
    final Estimator[][] completionInfo;

    /** For each type of job we know, the queue length on this worker. */
    private final WorkerQueueInfo[] workersQueueInfo;

    long timeStamp;

    /** The number of processors on this node. */
    private final int numberOfProcessors;

    NodePerformanceInfo(final Estimator[][] completionInfo,
            final WorkerQueueInfo[] workerQueueInfo,
            final IbisIdentifier source, final int numberOfProcessors,
            final long timeStamp) {
        this.completionInfo = completionInfo;
        this.workersQueueInfo = workerQueueInfo;
        this.source = source;
        this.numberOfProcessors = numberOfProcessors;
        this.timeStamp = timeStamp;
    }

    NodePerformanceInfo getDeepCopy() {
        final Estimator completionInfoCopy[][] = new Estimator[completionInfo.length][];

        for (int i = 0; i < completionInfo.length; i++) {
            completionInfoCopy[i] = Arrays.copyOf(completionInfo[i],
                    completionInfo[i].length);
        }
        final WorkerQueueInfo workerQueueInfoCopy[] = Arrays.copyOf(
                workersQueueInfo, workersQueueInfo.length);
        return new NodePerformanceInfo(completionInfoCopy, workerQueueInfoCopy,
                source, numberOfProcessors, timeStamp);
    }

    private String buildCompletionString() {
        final StringBuilder b = new StringBuilder("[");
        for (final Estimator[] l : completionInfo) {
            char sep = '[';
            for (final Estimator i : l) {
                b.append(sep);
                b.append(Utils.formatSeconds(i.getAverage()));
                sep = ',';
            }
            b.append(']');
        }
        return b.toString();
    }

    /**
     * Returns a string representation of update message. (Overrides method in
     * superclass.)
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        final String completion = buildCompletionString();
        final String workerQueue = Arrays.deepToString(workersQueueInfo);
        return "Update @" + timeStamp + " " + workerQueue + " " + completion;
    }

    Estimator estimateJobCompletion(final LocalNodeInfoList localNodeInfo,
            final JobType seriesType, final int stage, final JobType stageType,
            final boolean ignoreBusyProcessors) {
        if (localNodeInfo == null) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportInternalError("No local node info");
            }
            return null;
        }
        final WorkerQueueInfo workerQueueInfo = workersQueueInfo[stageType.index];

        if (workerQueueInfo == null) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportInternalError("Node " + source
                        + " does not provide queue info for type " + stageType);
            }
            return null;
        }
        if (localNodeInfo.suspect) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportError("Node " + source
                        + " is suspect, no completion estimate");
            }
            return null;
        }
        final Estimator completionInterval = completionInfo[seriesType.index][stage];
        if (completionInterval == null) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportError("Node " + source
                        + " has infinite completion time for seriesType="
                        + seriesType + " stage " + stage);
            }
            return null;
        }
        final LocalNodeInfo performanceInfo = localNodeInfo
                .getLocalNodeInfo(stageType);
        final int ql = workerQueueInfo.getQueueLength();
        // The estimated number of jobs on the node. The queue length `ql'
        // from the gossip is the most direct measure, but it might be stale.
        // We therefore also take our local count of outstanding jobs into
        // account.
        final int maximalQueueLength = stageType.unpredictable ? 0
                : Settings.MAXIMAL_QUEUE_FOR_PREDICTABLE;
        if (ignoreBusyProcessors
                && performanceInfo.currentJobs >= numberOfProcessors
                        + maximalQueueLength) {
            // Don't submit jobs, there are no idle processors.
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportProgress("Node " + source
                        + " has no idle processors for stage type " + stageType
                        + ": performanceInfo=" + performanceInfo);
            }
            return null;
        }
        // Give nodes already running jobs some penalty to encourage
        // spreading the load over nodes.
        final int currentJobs = Math.max(ql, performanceInfo.currentJobs);
        final Estimator executionTime = workerQueueInfo.getExecutionTime();
        final Estimator unpredictableOverhead = executionTime
                .multiply(currentJobs / 10);
        final Estimator transmissionTime = performanceInfo.transmissionTime;
        final int waitingJobs = Math.max(0, currentJobs - numberOfProcessors);
        final Estimator dequeueTimePerJob = workerQueueInfo
                .getDequeueTimePerJob();
        final Estimator queueTime = dequeueTimePerJob.multiply(waitingJobs);
        final Estimator transmissionAndQueueTime = transmissionTime
                .addIndependent(queueTime);
        final Estimator transmissionQueueAndExecutionTime = transmissionAndQueueTime
                .addIndependent(workerQueueInfo.getExecutionTime());
        final Estimator t1 = transmissionQueueAndExecutionTime
                .addIndependent(completionInterval);
        final Estimator total = t1.addIndependent(unpredictableOverhead);
        if (Settings.traceRemainingJobTime) {
            Globals.log.reportProgress("Estimated completion time for "
                    + source + " for " + seriesType + " stage " + stage + " "
                    + stageType + " is " + total.format()
                    + ": performanceInfo=" + performanceInfo);
        }
        return total;
    }

    /**
     * Given the index of a type, return the interval in seconds it will take
     * from the moment a job of this type arrives at the worker queue of this
     * node, until the entire job it belongs to is completed.
     * 
     * @param todoIx
     *            The index of the todo list of the overall type.
     * @param ix
     *            The index of the type we're interested in.
     * @param nextIx
     *            The index of the next type to the one we're interested in, so
     *            that we can get the (already calculated) completion time from
     *            that one. A value <code>-1</code> means there is no next type.
     */
    Estimator getCompletionOnWorker(final int todoIx, final int ix,
            final int nextIx) {
        final WorkerQueueInfo info = workersQueueInfo[ix];
        Estimator nextCompletionInterval;

        if (info == null) {
            // We don't support this type.
            return null;
        }
        if (nextIx >= 0) {
            final Estimator[] todoList = completionInfo[todoIx];
            nextCompletionInterval = todoList[nextIx];
        } else {
            nextCompletionInterval = ConstantEstimator.ZERO;
        }
        final Estimator dequeueTimePerJob = info.getDequeueTimePerJob();
        final Estimator totalDequeueTime = dequeueTimePerJob.multiply(1 + info
                .getQueueLength());
        final Estimator res = totalDequeueTime.addIndependent(
                info.getExecutionTime()).addIndependent(nextCompletionInterval);
        return res;
    }

    void print(final PrintStream s) {
        for (final WorkerQueueInfo i : workersQueueInfo) {
            if (i == null) {
                s.print(WorkerQueueInfo.emptyFormat());
            } else {
                s.print(i.format());
                s.print(' ');
            }
        }
        s.print(" | ");
        for (final Estimator l[] : completionInfo) {
            s.print('[');
            boolean first = true;
            for (final Estimator t : l) {
                if (first) {
                    first = false;
                } else {
                    s.print(' ');
                }
                s.printf("%8s", Utils.formatSeconds(t.getAverage()));
            }
            s.print(']');
        }
        s.println(source);
    }

    static void printTopLabel(final PrintStream s, final JobList jobs) {
        final JobType[] allJobTypes = jobs.getAllTypes();
        for (final JobType t : allJobTypes) {
            s.print(WorkerQueueInfo.topLabelType(t));
            s.print(' ');
        }
        s.print(" | ");
        for (final JobType t : allJobTypes) {
            s.printf("%8s ", t);
        }
        s.println();
        for (@SuppressWarnings("unused")
        final JobType t : allJobTypes) {
            s.print(WorkerQueueInfo.topLabel());
            s.print(' ');
        }
        s.print(" | ");
        s.println();
    }

    void failJob(final JobType type) {
        final WorkerQueueInfo info = workersQueueInfo[type.index];
        if (info != null) {
            info.failJob();
            timeStamp = System.nanoTime();
        }
    }

    void setComputeTime(final JobType type, final Estimator t) {
        final WorkerQueueInfo info = workersQueueInfo[type.index];
        if (info != null) {
            info.setExecutionTime(t);
            timeStamp = System.nanoTime();
        }
    }

    void setWorkerQueueTimePerJob(final JobType type,
            final Estimator queueTimePerJob, final int queueLength) {
        final WorkerQueueInfo info = workersQueueInfo[type.index];
        if (info != null) {
            info.setQueueTimePerJob(queueTimePerJob, queueLength);
            timeStamp = System.nanoTime();
        }
    }

    boolean setWorkerQueueLength(final JobType type, final int newQueueLength) {
        boolean changed = false;
        final WorkerQueueInfo info = workersQueueInfo[type.index];
        if (info != null) {
            changed = info.setQueueLength(newQueueLength);
            timeStamp = System.nanoTime();
        }
        return changed;
    }
}
