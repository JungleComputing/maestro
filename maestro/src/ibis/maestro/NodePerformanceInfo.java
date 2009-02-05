package ibis.maestro;

import ibis.ipl.IbisIdentifier;

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
     * For each type of task we know, the estimated time it will take to
     * complete the remaining tasks of this job.
     */
    final double[] completionInfo;

    /** For each type of task we know, the queue length on this worker. */
    WorkerQueueInfo[] workerQueueInfo;

    long timeStamp;

    final int numberOfProcessors;

    NodePerformanceInfo(double[] completionInfo,
            WorkerQueueInfo[] workerQueueInfo, IbisIdentifier source,
            int numberOfProcessors, long timeStamp) {
        this.completionInfo = completionInfo;
        this.workerQueueInfo = workerQueueInfo;
        this.source = source;
        this.numberOfProcessors = numberOfProcessors;
        this.timeStamp = timeStamp;
    }

    NodePerformanceInfo getDeepCopy() {
        final double completionInfoCopy[] = Arrays.copyOf(completionInfo,
                completionInfo.length);
        final WorkerQueueInfo workerQueueInfoCopy[] = Arrays.copyOf(
                workerQueueInfo, workerQueueInfo.length);
        return new NodePerformanceInfo(completionInfoCopy, workerQueueInfoCopy,
                source, numberOfProcessors, timeStamp);
    }

    private String buildCompletionString() {
        final StringBuilder b = new StringBuilder("[");
        for (final double i : completionInfo) {
            b.append(Utils.formatSeconds(i));
        }
        b.append(']');
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
        final String workerQueue = Arrays.deepToString(workerQueueInfo);
        return "Update @" + timeStamp + " " + workerQueue + " " + completion;
    }

    double estimateJobCompletion(LocalNodeInfo localNodeInfo, TaskType type,
            boolean ignoreBusyProcessors) {
        if (localNodeInfo == null) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportError("No local node info");
            }
            return Double.POSITIVE_INFINITY;
        }
        final WorkerQueueInfo queueInfo = workerQueueInfo[type.index];
        final double completionInterval = completionInfo[type.index];
        double unpredictableOverhead = 0L;

        if (queueInfo == null) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportError("Node " + source
                        + " does not provide queue info for type " + type);
            }
            return Double.POSITIVE_INFINITY;
        }
        if (localNodeInfo.suspect) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportError("Node " + source
                        + " is suspect, no completion estimate");
            }
            return Double.POSITIVE_INFINITY;
        }
        if (completionInterval == Double.POSITIVE_INFINITY) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportError("Node " + source
                        + " has infinite completion time");
            }
            return Double.POSITIVE_INFINITY;
        }
        final int currentTasks = localNodeInfo.getCurrentTasks(type);
        final int maximalQueueLength = type.unpredictable ? 0
                : Settings.MAXIMAL_QUEUE_FOR_PREDICTABLE;
        if (ignoreBusyProcessors
                && currentTasks >= (numberOfProcessors + maximalQueueLength)) {
            // Don't submit jobs, there are no idle processors.
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportError("Node " + source
                        + " has no idle processors");
            }
            return Double.POSITIVE_INFINITY;
        }
        // Give nodes already running tasks some penalty to encourage
        // spreading the load over nodes.
        final double executionTime = queueInfo.getExecutionTime();
        unpredictableOverhead = (currentTasks * executionTime) / 10;
        final double transmissionTime = localNodeInfo.getTransmissionTime(type);
        final int waitingTasks = Math.max(0,
                (1 + queueInfo.getQueueLength() + currentTasks)
                        - numberOfProcessors);
        final double total = transmissionTime + waitingTasks
                * queueInfo.getDequeueTimePerTask() + queueInfo
                .getExecutionTime() + completionInterval + unpredictableOverhead;
        if (Settings.traceRemainingJobTime) {
            Globals.log.reportProgress("Estimated completion time for "
                    + source + " is " + Utils.formatSeconds(total));
        }
        return total;
    }

    /**
     * Given the index of a type, return the interval in nanoseconds it will
     * take from the moment a task of this type arrives at the worker queue of
     * this node, until the entire job it belongs to is completed.
     * 
     * @param ix
     *            The index of the type we're interested in.
     * @param nextIx
     *            The index of the next type to the one we're interested in, so
     *            that we can get the (already calculated) completion time from
     *            that one. A value <code>-1</code> means there is no next
     *            type.
     */
    double getCompletionOnWorker(int ix, int nextIx) {
        final WorkerQueueInfo info = workerQueueInfo[ix];
        double nextCompletionInterval;

        if (info == null) {
            // We don't support this type.
            return Double.POSITIVE_INFINITY;
        }
        if (nextIx >= 0) {
            nextCompletionInterval = completionInfo[nextIx];
        } else {
            nextCompletionInterval = 0.0;
        }
        return (1 + info.getQueueLength())
                * info.getDequeueTimePerTask() + info.getExecutionTime() +
                nextCompletionInterval;
    }

    void print(PrintStream s) {
        for (final WorkerQueueInfo i : workerQueueInfo) {
            if (i == null) {
                s.print(WorkerQueueInfo.emptyFormat());
            } else {
                s.print(i.format());
                s.print(' ');
            }
        }
        s.print(" | ");
        for (final double t : completionInfo) {
            s.printf("%8s ", Utils.formatSeconds(t));
        }
        s.println(source);
    }

    static void printTopLabel(PrintStream s) {
        for (final TaskType t : Globals.allTaskTypes) {
            s.print(WorkerQueueInfo.topLabelType(t));
            s.print(' ');
        }
        s.print(" | ");
        for (final TaskType t : Globals.allTaskTypes) {
            s.printf("%8s ", t);
        }
        s.println();
        for (@SuppressWarnings("unused")
        final TaskType t : Globals.allTaskTypes) {
            s.print(WorkerQueueInfo.topLabel());
            s.print(' ');
        }
        s.print(" | ");
        s.println();
    }

    void failTask(TaskType type) {
        final WorkerQueueInfo info = workerQueueInfo[type.index];
        if (info != null) {
            info.failTask();
            timeStamp = System.nanoTime();
        }
    }

    void setComputeTime(TaskType type, double t) {
        final WorkerQueueInfo info = workerQueueInfo[type.index];
        if (info != null) {
            info.setExecutionTime(t);
            timeStamp = System.nanoTime();
        }
    }

    void setWorkerQueueTimePerTask(TaskType type, double queueTimePerTask,
            int queueLength) {
        final WorkerQueueInfo info = workerQueueInfo[type.index];
        if (info != null) {
            info.setQueueTimePerTask(queueTimePerTask, queueLength);
            timeStamp = System.nanoTime();
        }
    }

    void setWorkerQueueLength(TaskType type, int newQueueLength) {
        final WorkerQueueInfo info = workerQueueInfo[type.index];
        if (info != null) {
            info.setQueueLength(newQueueLength);
            timeStamp = System.nanoTime();
        }
    }
}
