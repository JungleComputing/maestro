package ibis.maestro;

import ibis.steel.Estimate;
import ibis.steel.LogGaussianEstimate;

import java.io.PrintStream;
import java.util.ArrayList;

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
final class WorkerQueue {
    private final ArrayList<RunJobMessage> queue = new ArrayList<RunJobMessage>();

    private final WorkerQueueJobInfo queueTypes[];

    private double activeTime = 0.0;

    /**
     * Given a list of supported types, constructs a new WorkerQueue.
     * 
     * @param jobs
     *            The list of job types we support.
     */
    WorkerQueue(final JobList jobs) {
        final JobType[] jobTypes = jobs.getAllTypes();
        queueTypes = new WorkerQueueJobInfo[jobTypes.length];
        for (final JobType t : jobTypes) {
            final Job job = jobs.getJob(t);
            Estimate est;
            if (job instanceof JobExecutionTimeEstimator) {
                final JobExecutionTimeEstimator estimator = (JobExecutionTimeEstimator) job;
                est = estimator.estimateJobExecutionTime();
                if (est == null) {
                    Globals.log
                            .reportInternalError("estimateExecutionTime() should not return null. jobtype="
                                    + job.getClass());
                }
            } else {
                est = new LogGaussianEstimate(Math.log(1 * Utils.MILLISECOND),
                        Math.log(100), 1);
            }
            final WorkerQueueJobInfo queueTypeInfo = new WorkerQueueJobInfo(t,
                    est);
            queueTypes[t.index] = queueTypeInfo;

        }
    }

    /**
     * Returns true iff the entire queue is empty.
     * 
     * @return
     */
    synchronized boolean isEmpty() {
        return queue.isEmpty();
    }

    private static int findInsertionPoint(final ArrayList<RunJobMessage> queue,
            final RunJobMessage msg) {
        // Good old binary search.
        int start = 0;
        int end = queue.size();
        if (end == 0) {
            // The queue is empty. This is the only case where start
            // points to a non-existent element, so we have to treat
            // it separately.
            return 0;
        }
        final long ids[] = msg.jobInstance.jobInstance.ids;
        while (true) {
            final int mid = (start + end) / 2;
            if (mid == start) {
                break;
            }
            final long midIds[] = queue.get(mid).jobInstance.jobInstance.ids;
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
        final long startIds[] = queue.get(start).jobInstance.jobInstance.ids;
        final int cmp = Utils.compareIds(startIds, ids);
        if (cmp < 0) {
            return end;
        }
        return start;
    }

    private void dumpQueue() {
        Globals.log.reportProgress("Worker queue: ");
        final PrintStream s = Globals.log.getPrintStream();
        for (final RunJobMessage m : queue) {
            s.print(m.label());
            s.print(' ');
        }
        s.println();
    }

    /**
     * Add the given job to our queue.
     * 
     * @param msg
     *            The job to add to the queue
     */
    int add(final JobType type, final RunJobMessage msg) {
        final int length;
        final WorkerQueueJobInfo info = queueTypes[type.index];
        final int pos;
        synchronized (this) {
            if (activeTime == 0.0) {
                activeTime = msg.arrivalMoment;
            }
            length = info.registerAdd();
            pos = findInsertionPoint(queue, msg);
            queue.add(pos, msg);
        }
        if (Settings.traceQueuing) {
            Globals.log.reportProgress("Adding "
                    + msg.jobInstance.formatJobAndType() + " at position "
                    + pos + " of worker queue; length is now " + queue.size()
                    + "; " + length + " of type " + type);
        }
        if (Settings.dumpWorkerQueue) {
            dumpQueue();
        }
        return length;
    }

    RunJobMessage remove(final JobList jobs, final Gossiper gossiper) {
        final RunJobMessage res;
        final int length;
        final WorkerQueueJobInfo info;
        final JobType type;

        synchronized (this) {
            if (queue.isEmpty()) {
                return null;
            }
            res = queue.remove(0);
            type = res.jobInstance.getStageType(jobs);
            info = queueTypes[type.index];
            length = info.registerRemove();
        }
        if (Settings.traceQueuing) {
            Globals.log.reportProgress("Removing "
                    + res.jobInstance.formatJobAndType()
                    + " from worker queue; length is now " + queue.size()
                    + "; " + length + " of type " + type);
        }
        if (gossiper != null) {
            final Estimate queueTimePerJob = info.getQueueTimePerJob();
            gossiper.setWorkerQueueTimePerJob(type, queueTimePerJob, length);
        }
        return res;
    }

    boolean failJob(final JobType type) {
        final WorkerQueueJobInfo info = queueTypes[type.index];
        info.failJob();

        synchronized (this) {
            for (final WorkerQueueJobInfo i : queueTypes) {
                if (i != null && !i.hasFailed()) {
                    return false; // There still is a non-failed job type.
                }
            }
        }
        return true; // All job types have failed.
    }

    Estimate countJob(final JobType type, final double computeInterval) {
        final WorkerQueueJobInfo info = queueTypes[type.index];
        return info.countJob(computeInterval, type.unpredictable);
    }

    synchronized double getActiveTime(final double startTime) {
        if (activeTime < startTime) {
            Globals.log.reportProgress("Worker was not used");
            return startTime;
        }
        return activeTime;
    }

    synchronized void printStatistics(final PrintStream s,
            final double workInterval) {
        for (final WorkerQueueJobInfo t : queueTypes) {
            if (t != null) {
                t.printStatistics(s, workInterval);
            }
        }
    }

    void registerNode(final WorkerInfo nodeInfo) {
        for (final WorkerQueueJobInfo info : queueTypes) {
            if (info != null) {
                info.registerNode(nodeInfo);
            }
        }
    }

    WorkerQueueJobInfo getJobInfo(final int ix) {
        return queueTypes[ix];
    }

    synchronized void clear() {
        queue.clear();
    }
}
