package ibis.maestro;

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
    WorkerQueue(JobList jobs) {
        final JobType[] jobTypes = Globals.allJobTypes;
        queueTypes = new WorkerQueueJobInfo[jobTypes.length];
        for (final JobType t : jobTypes) {
            final WorkerQueueJobInfo queueTypeInfo = new WorkerQueueJobInfo(t);
            queueTypes[t.index] = queueTypeInfo;
            final Job job = jobs.getJob(t);
            if (job instanceof JobExecutionTimeEstimator) {
                final JobExecutionTimeEstimator estimator = (JobExecutionTimeEstimator) job;
                queueTypeInfo.setInitialComputeTimeEstimate(estimator
                        .estimateJobExecutionTime());
            }

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

    private static int findInsertionPoint(ArrayList<RunJobMessage> queue,
            RunJobMessage msg) {
        // Good old binary search.
        int start = 0;
        int end = queue.size();
        if (end == 0) {
            // The queue is empty. This is the only case where start
            // points to a non-existent element, so we have to treat
            // it separately.
            return 0;
        }
        final long id = msg.jobInstance.jobInstance.id;
        while (true) {
            final int mid = (start + end) / 2;
            if (mid == start) {
                break;
            }
            final long midId = queue.get(mid).jobInstance.jobInstance.id;
            if (midId < id) {
                // Mid should come before us.
                start = mid;
            } else {
                // Mid should come after us.
                end = mid;
            }
        }
        // This comparison is probably rarely necessary, but corner cases
        // are a pain, so I'm safe rather than sorry.
        final long startId = queue.get(start).jobInstance.jobInstance.id;
        if (startId < id) {
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
    int add(RunJobMessage msg) {
        final int length;
        final JobType type = msg.jobInstance.type;
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

    RunJobMessage remove(Gossiper gossiper) {
        final RunJobMessage res;
        final int length;
        final WorkerQueueJobInfo info;

        synchronized (this) {
            if (queue.isEmpty()) {
                return null;
            }
            res = queue.remove(0);
            info = queueTypes[res.jobInstance.type.index];
            length = info.registerRemove();
        }
        if (Settings.traceQueuing) {
            Globals.log.reportProgress("Removing "
                    + res.jobInstance.formatJobAndType()
                    + " from worker queue; length is now " + queue.size()
                    + "; " + length + " of type " + res.jobInstance.type);
        }
        if (gossiper != null) {
            final double queueTimePerJob = info.getDequeueInterval();
            gossiper.setWorkerQueueTimePerJob(res.jobInstance.type,
                    queueTimePerJob, length);
        }
        return res;
    }

    boolean failJob(JobType type) {
        final WorkerQueueJobInfo info = queueTypes[type.index];
        info.failJob();

        // TODO: synchronize this properly; due to race conditions the last two
        // job types may be failed at the same time without either one
        // returning false.
        for (final WorkerQueueJobInfo i : queueTypes) {
            if (i != null && !i.hasFailed()) {
                return false; // There still is a non-failed job type.
            }
        }
        return true; // All job types have failed.
    }

    double countJob(JobType type, double computeInterval) {
        final WorkerQueueJobInfo info = queueTypes[type.index];
        return info.countJob(computeInterval, type.unpredictable);
    }

    synchronized double getActiveTime(double startTime) {
        if (activeTime < startTime) {
            Globals.log.reportProgress("Worker was not used");
            return startTime;
        }
        return activeTime;
    }

    synchronized void printStatistics(PrintStream s, double workInterval) {
        for (final WorkerQueueJobInfo t : queueTypes) {
            if (t != null) {
                t.printStatistics(s, workInterval);
            }
        }
    }

    WorkerQueueJobInfo getJobInfo(JobType type) {
        return queueTypes[type.index];
    }

    void registerNode(NodeInfo nodeInfo) {
        for (final WorkerQueueJobInfo info : queueTypes) {
            if (info != null) {
                info.registerNode(nodeInfo);
            }
        }
    }

    WorkerQueueJobInfo getJobInfo(int ix) {
        return queueTypes[ix];
    }

    synchronized void clear() {
        queue.clear();
    }
}
