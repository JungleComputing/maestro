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
     * For each type of job we know, the estimated time it will take to
     * complete the remaining jobs of this job.
     */
    final double[][] completionInfo;

    /** For each type of job we know, the queue length on this worker. */
    private final WorkerQueueInfo[] workersQueueInfo;

    long timeStamp;

    /** The number of processors on this node. */
    private final int numberOfProcessors;

    NodePerformanceInfo(double[][] completionInfo,
            WorkerQueueInfo[] workerQueueInfo, IbisIdentifier source,
            int numberOfProcessors, long timeStamp) {
        this.completionInfo = completionInfo;
        this.workersQueueInfo = workerQueueInfo;
        this.source = source;
        this.numberOfProcessors = numberOfProcessors;
        this.timeStamp = timeStamp;
    }

    NodePerformanceInfo getDeepCopy() {
        final double completionInfoCopy[][] = new double[completionInfo.length][];
        for( int i=0; i<completionInfo.length; i++ ){
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
        for( final double l[]: completionInfo){
            char sep = '[';
            for (final double i : l) {
                b.append(sep);
                b.append(Utils.formatSeconds(i));
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

    double estimateJobCompletion(LocalNodeInfo localNodeInfo, JobType seriesType,int stage,
            JobType stageType,
            boolean ignoreBusyProcessors) {
        if (localNodeInfo == null) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportInternalError("No local node info");
            }
            return Double.POSITIVE_INFINITY;
        }
        final WorkerQueueInfo workerQueueInfo = workersQueueInfo[stageType.index];

        if (workerQueueInfo == null) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportInternalError("Node " + source
                        + " does not provide queue info for type " + stageType);
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
        final double completionInterval = completionInfo[seriesType.index][stage];
        if (completionInterval == Double.POSITIVE_INFINITY) {
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportError("Node " + source
                        + " has infinite completion time");
            }
            return Double.POSITIVE_INFINITY;
        }
        final int currentJobs = localNodeInfo.getCurrentJobs(stageType);
        final int maximalQueueLength = stageType.unpredictable ? 0
                : Settings.MAXIMAL_QUEUE_FOR_PREDICTABLE;
        if (ignoreBusyProcessors
                && currentJobs >= (numberOfProcessors + maximalQueueLength)) {
            // Don't submit jobs, there are no idle processors.
            if (Settings.traceRemainingJobTime) {
                Globals.log.reportError("Node " + source
                        + " has no idle processors for type " + stageType );
            }
            return Double.POSITIVE_INFINITY;
        }
        // Give nodes already running jobs some penalty to encourage
        // spreading the load over nodes.
        final double executionTime = workerQueueInfo.getExecutionTime();
        final double unpredictableOverhead = (currentJobs * executionTime) / 10;
        final double transmissionTime = localNodeInfo.getTransmissionTime(stageType);
        final int waitingJobs = 1+workerQueueInfo.getQueueLength();
        final double total = transmissionTime + waitingJobs
        * workerQueueInfo.getDequeueTimePerJob() + workerQueueInfo
        .getExecutionTime() + completionInterval + unpredictableOverhead;
        if (Settings.traceRemainingJobTime) {
            Globals.log.reportProgress("Estimated completion time for "
                    + source + " is " + Utils.formatSeconds(total));
        }
        return total;
    }

    /**
     * Given the index of a type, return the interval in seconds it will
     * take from the moment a job of this type arrives at the worker queue of
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
    double getCompletionOnWorker(int todoIx, int ix, int nextIx) {
        final WorkerQueueInfo info = workersQueueInfo[ix];
        double nextCompletionInterval;

        if (info == null) {
            // We don't support this type.
            return Double.POSITIVE_INFINITY;
        }
        if (nextIx >= 0) {
            nextCompletionInterval = completionInfo[todoIx][nextIx];
        } else {
            nextCompletionInterval = 0.0;
        }
        return (1 + info.getQueueLength())
        * info.getDequeueTimePerJob() + info.getExecutionTime() +
        nextCompletionInterval;
    }

    void print(PrintStream s) {
        for (final WorkerQueueInfo i : workersQueueInfo) {
            if (i == null) {
                s.print(WorkerQueueInfo.emptyFormat());
            } else {
                s.print(i.format());
                s.print(' ');
            }
        }
        s.print(" | ");
        for (final double l[]: completionInfo){
            s.print('[');
            boolean first = true;
            for (final double t : l) {
                if( first ){
                    first = false;
                }
                else {
                    s.print( ' ' );
                }
                s.printf("%8s", Utils.formatSeconds(t));
            }
            s.print(']');
        }
        s.println(source);
    }

    static void printTopLabel(PrintStream s,JobList jobs) {
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

    void failJob(JobType type) {
        final WorkerQueueInfo info = workersQueueInfo[type.index];
        if (info != null) {
            info.failJob();
            timeStamp = System.nanoTime();
        }
    }

    void setComputeTime(JobType type, double t) {
        final WorkerQueueInfo info = workersQueueInfo[type.index];
        if (info != null) {
            info.setExecutionTime(t);
            timeStamp = System.nanoTime();
        }
    }

    void setWorkerQueueTimePerJob(JobType type, double queueTimePerJob,
            int queueLength) {
        final WorkerQueueInfo info = workersQueueInfo[type.index];
        if (info != null) {
            info.setQueueTimePerJob(queueTimePerJob, queueLength);
            timeStamp = System.nanoTime();
        }
    }

    boolean setWorkerQueueLength(JobType type, int newQueueLength) {
        boolean changed = false;
        final WorkerQueueInfo info = workersQueueInfo[type.index];
        if (info != null) {
            changed = info.setQueueLength(newQueueLength);
            timeStamp = System.nanoTime();
        }
        return changed;
    }
}
