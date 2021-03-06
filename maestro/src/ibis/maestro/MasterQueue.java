package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.steel.Estimate;
import ibis.steel.Estimator;
import ibis.steel.LogGaussianDecayingEstimator;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * A class representing the master work queue.
 * 
 * This requires a special implementation because we want to enforce priorities
 * for the different job types, and we want to know which job types are
 * currently present in the queue.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class MasterQueue {
    private int jobCount = 0;

    /**
     * For each known type, some administration about its presence in the queue.
     */
    private final TypeInfo queueTypes[];

    /** The queue itself. */
    private final ArrayList<JobInstance> queue = new ArrayList<JobInstance>();

    /**
     * Statistics per type for the different job types in the queue.
     * 
     * @author Kees van Reeuwijk
     */
    private static final class TypeInfo {
        /** The type these statistics are about. */
        private final JobType type;

        /** The total number of jobs of this type that ever entered the queue. */
        private long jobCount = 0;

        /** Current number of elements of this type in the queue. */
        private int elements = 0;

        /** Maximal ever number of elements ever in the queue. */
        private int maxElements = 0;

        /** Last time the front of the queue changed. */
        private double frontChangedTime = 0;

        /** The estimated time interval between jobs being dequeued. */
        private final Estimator dequeueInterval;

        private TypeInfo(final JobType type) {
            this.type = type;
            final double av = Math.log(1 * Utils.MILLISECOND);
            dequeueInterval = new LogGaussianDecayingEstimator(av,
                    Math.log(10), 0.1);
        }

        private synchronized void printStatistics(final PrintStream s) {
            s.println("master queue for " + type + ": " + jobCount
                    + " jobs; dequeue interval: " + dequeueInterval
                    + "; maximal queue size: " + maxElements);
        }

        synchronized private int administrateAdd() {
            elements++;
            if (elements > maxElements) {
                maxElements = elements;
            }
            if (frontChangedTime == 0.0) {
                // This entry is the front of the queue,
                // record the time it became this.
                frontChangedTime = Utils.getPreciseTime();
            }
            jobCount++;
            return elements;
        }

        private void administrateRemove() {
            final double now = Utils.getPreciseTime();
            synchronized (this) {
                if (frontChangedTime != 0) {
                    // We know when this entry became the front of the queue.
                    final double i = now - frontChangedTime;
                    dequeueInterval.addSample(i);
                }
                elements--;
                if (elements == 0) {
                    // Don't take the next dequeuing into account,
                    // since the queue is now empty.
                    frontChangedTime = 0l;
                } else {
                    frontChangedTime = now;
                }
            }
        }

        /**
         * Estimate the time a new job will spend in the queue.
         * 
         * @return The estimated time in seconds a new job will spend in the
         *         queue.
         */
        private synchronized Estimate estimateQueueTime() {
            final Estimate timePerEntry = dequeueInterval.getEstimate();
            // Since at least one processor isn't working on a job (or we
            // wouldn't be here), we are only impressed if there is more
            // than one idle processor.
            final Estimate res = timePerEntry.multiply(1 + elements);
            return res;
        }
    }

    /**
     * Constructs a new MasterQueue.
     * 
     * @param jobTypes
     *            The supported types.
     */
    @SuppressWarnings("synthetic-access")
    MasterQueue(final JobType allTypes[]) {
        queueTypes = new TypeInfo[allTypes.length];
        for (final JobType type : allTypes) {
            queueTypes[type.index] = new TypeInfo(type);
        }
    }

    private static int findInsertionPoint(final ArrayList<JobInstance> queue,
            final JobInstance e) {
        // Good old binary search.
        int start = 0;
        int end = queue.size();
        if (end == 0) {
            // The queue is empty. This is the only case where start
            // points to a non-existent element, so we have to treat
            // it separately.
            return 0;
        }
        final long ids[] = e.jobInstance.ids;
        while (true) {
            final int mid = (start + end) / 2;
            if (mid == start) {
                break;
            }
            final long midIds[] = queue.get(mid).jobInstance.ids;
            final int cmp = Utils.compareIds(midIds, ids);
            if (cmp < 0) {
                // Mid should come before us.
                start = mid;
            } else {
                // Mid should come after us.
                end = mid;
            }
        }
        // This comparison is probably rarely necessary, but corner cases
        // are a pain, so I'm safe rather than sorry.
        final long startIds[] = queue.get(start).jobInstance.ids;
        final int cmp = Utils.compareIds(startIds, ids);
        if (cmp < 0) {
            return end;
        }
        return start;
    }

    /**
     * Returns true iff the entire queue is empty.
     * 
     * @return <code>true</code> iff the entire master queue is empty.
     */
    synchronized boolean isEmpty() {
        return queue.isEmpty();
    }

    private void dumpQueue(final PrintStream s) {
        s.print("Jobs in master queue: ");
        for (final JobInstance e : queue) {
            s.print(e.shortLabel());
            s.print(' ');
        }
        s.println();
    }

    /**
     * Submit a new job, belonging to the job with the given identifier, to the
     * queue.
     * 
     * @param job
     *            The job to submit.
     */
    @SuppressWarnings("synthetic-access")
    protected synchronized void add(final JobList jobs, final JobInstance job) {
        jobCount++;
        final int pos = findInsertionPoint(queue, job);
        queue.add(pos, job);

        final JobType type = job.getStageType(jobs);
        final TypeInfo info = queueTypes[type.index];
        final int length = info.administrateAdd();
        if (Settings.traceQueuing) {
            Globals.log.reportProgress("Adding " + job.formatJobAndType()
                    + " at position " + pos
                    + " of master queue; length is now " + queue.size() + "; "
                    + length + " of type " + type);
        }
        if (Settings.dumpMasterQueue) {
            dumpQueue(Globals.log.getPrintStream());
        }
    }

    /**
     * Adds all the job instances in the given list to the queue. The list may
     * be <code>null</code>.
     * 
     * @param l
     *            The list of job instances to add.
     */
    void add(final JobList jobs, final ArrayList<JobInstance> l) {
        if (l != null) {
            for (final JobInstance job : l) {
                add(jobs, job);
            }
        }
    }

    @SuppressWarnings("synthetic-access")
    synchronized void printStatistics(final PrintStream s) {
        for (final TypeInfo t : queueTypes) {
            if (t != null) {
                t.printStatistics(s);
            }
        }
        s.printf("Master: # incoming jobs = %5d\n", jobCount);
    }

    /**
     * Given a job type, select the best worker from the list that has a free
     * slot. In this context 'best' is simply the worker with the shortest
     * overall completion time.
     * 
     * @param jobs
     *            Information about job types.
     * @param localNodeInfoMap
     *            Local information about all nodes
     * @param tables
     *            Globally known information about all nodes
     * @param jobs
     *            Information about the different types of jobs we support.
     * @param job
     *            The job instance we want a worker for.
     * 
     * @return The info of the best worker for this job, or <code>null</code> if
     *         there currently aren't any workers for this job type.
     */
    private Submission selectBestWorker(
            final HashMap<IbisIdentifier, LocalNodeInfoList> localNodeInfoMap,
            final NodePerformanceInfo tables[], final JobInstance job,
            final JobType stageType) {
        NodePerformanceInfo best = null;
        double bestInterval = Double.POSITIVE_INFINITY;

        for (final NodePerformanceInfo info : tables) {
            final LocalNodeInfoList localNodeInfo = localNodeInfoMap
                    .get(info.source);

            final Estimate est = info.estimateJobCompletion(localNodeInfo,
                    job.overallType, job.stageNumber, stageType,
                    Settings.HARD_ALLOWANCES);
            if (est != null) {
                final double val = est.getLikelyValue();

                if (val < bestInterval) {
                    bestInterval = val;
                    best = info;
                }
            }
        }
        if (Settings.traceWorkerSelection) {
            dumpChoices(localNodeInfoMap, tables, job, stageType, best);
        }
        if (best == null) {
            if (Settings.traceMasterQueue) {
                Globals.log.reportProgress("No workers for job of type "
                        + stageType);
            }
            return null;
        }
        if (Settings.traceMasterQueue) {
            Globals.log.reportProgress("Selected worker " + best.source
                    + " for job of type " + stageType);
        }
        final LocalNodeInfoList localNodeInfo = localNodeInfoMap
                .get(best.source);
        final double predictedDuration = localNodeInfo.getDeadline(stageType);
        return new Submission(job, best.source, predictedDuration);
    }

    private static void dumpChoices(
            final HashMap<IbisIdentifier, LocalNodeInfoList> localNodeInfoMap,
            final NodePerformanceInfo[] tables, final JobInstance job,
            final JobType stageType, final NodePerformanceInfo bestInterval) {
        final PrintStream s = Globals.log.getPrintStream();
        for (final NodePerformanceInfo i : tables) {
            i.print(s);
        }
        s.print("Best worker: ");
        for (final NodePerformanceInfo info : tables) {
            final LocalNodeInfoList localNodeInfo = localNodeInfoMap
                    .get(info.source);
            final Estimate val = info.estimateJobCompletion(localNodeInfo,
                    job.overallType, job.stageNumber, stageType, true);
            if (info == bestInterval) {
                s.print('#');
            }
            s.print(val == null ? "(busy)" : val.toString());
            s.print(' ');
        }
        s.println();
    }

    /**
     * Get a job submission from the queue. The entries are tried from front to
     * back, and the first one for which a worker can be found is returned.
     * 
     * @param tables
     *            Timing tables for the different workers.
     * @return A job submission, or <code>null</code> if there are no free
     *         workers for any of the jobs in the queue.
     * 
     */
    @SuppressWarnings("synthetic-access")
    synchronized Submission getSubmission(final JobList jobs,
            final HashMap<IbisIdentifier, LocalNodeInfoList> localNodeInfoMap,
            final NodePerformanceInfo[] tables) {

        final boolean skip[] = new boolean[queueTypes.length];
        int ix = 0;
        while (ix < queue.size()) {
            final JobInstance job = queue.get(ix);
            final JobType stageType = job.getStageType(jobs);
            final int typeIndex = stageType.index;
            if (!skip[typeIndex]) {
                final Submission sub = selectBestWorker(localNodeInfoMap,
                        tables, job, stageType);
                if (sub != null) {
                    queue.remove(ix);
                    final TypeInfo queueTypeInfo = queueTypes[stageType.index];
                    queueTypeInfo.administrateRemove();
                    if (Settings.traceMasterQueue || Settings.traceQueuing) {
                        final int length = queueTypeInfo.elements;

                        Globals.log.reportProgress("Removing "
                                + job.formatJobAndType()
                                + " from master queue; length is now "
                                + queue.size() + "; " + length + " of type "
                                + stageType);
                    }
                    return sub;
                }
            }
            skip[typeIndex] = true;
            if (Settings.traceMasterQueue) {
                Globals.log.reportProgress("No ready worker for job type "
                        + stageType);
            }
            ix++;
        }

        return null;
    }

    public JobInstance remove() {
        if (queue.size() < 1) {
            return null;
        }
        final JobInstance res = queue.remove(0);
        return res;
    }

    @SuppressWarnings("synthetic-access")
    Estimate[] getQueueIntervals() {
        final Estimate[] res = new Estimate[queueTypes.length];

        for (int ix = 0; ix < queueTypes.length; ix++) {
            res[ix] = queueTypes[ix].estimateQueueTime();
        }
        return res;
    }

    /**
     * Clear the work queue.
     * 
     */
    synchronized void clear() {
        queue.clear();
    }

    /**
     * Remove any copies of the given job instance from the master queue;
     * somebody already completed it.
     * 
     * @param job
     *            The job to remove.
     */
    synchronized void removeDuplicates(final JobInstance job) {
        while (queue.remove(job)) {
            // Nothing, but repeat until all have been removed.
        }
    }

    synchronized boolean hasRoom() {
        return queue.size() < Settings.MAESTRO_MASTER_ROOM;
    }
}
