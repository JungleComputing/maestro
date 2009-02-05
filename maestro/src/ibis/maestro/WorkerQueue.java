package ibis.maestro;

import java.io.PrintStream;
import java.util.ArrayList;

/**
 * A class representing the master work queue.
 * 
 * This requires a special implementation because we want to enforce priorities
 * for the different task types, and we want to know which task types are
 * currently present in the queue.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class WorkerQueue {
    private final ArrayList<RunTaskMessage> queue = new ArrayList<RunTaskMessage>();

    private final WorkerQueueTaskInfo queueTypes[];

    private long activeTime = 0L;

    /**
     * Given a list of supported types, constructs a new WorkerQueue.
     * 
     * @param jobs
     *            The list of job types we support.
     */
    WorkerQueue(JobList jobs) {
        final TaskType[] taskTypes = Globals.allTaskTypes;
        queueTypes = new WorkerQueueTaskInfo[taskTypes.length];
        for (final TaskType t : taskTypes) {
            final WorkerQueueTaskInfo queueTypeInfo = new WorkerQueueTaskInfo(t);
            queueTypes[t.index] = queueTypeInfo;
            final Task task = jobs.getTask(t);
            if (task instanceof TaskExecutionTimeEstimator) {
                final TaskExecutionTimeEstimator estimator = (TaskExecutionTimeEstimator) task;
                queueTypeInfo.setInitialComputeTimeEstimate(estimator
                        .estimateTaskExecutionTime());
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

    private static int findInsertionPoint(ArrayList<RunTaskMessage> queue,
            RunTaskMessage msg) {
        // Good old binary search.
        int start = 0;
        int end = queue.size();
        if (end == 0) {
            // The queue is empty. This is the only case where start
            // points to a non-existent element, so we have to treat
            // it separately.
            return 0;
        }
        final long id = msg.taskInstance.jobInstance.id;
        while (true) {
            final int mid = (start + end) / 2;
            if (mid == start) {
                break;
            }
            final long midId = queue.get(mid).taskInstance.jobInstance.id;
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
        final long startId = queue.get(start).taskInstance.jobInstance.id;
        if (startId < id) {
            return end;
        }
        return start;
    }

    private void dumpQueue() {
        Globals.log.reportProgress("Worker queue: ");
        final PrintStream s = Globals.log.getPrintStream();
        for (final RunTaskMessage m : queue) {
            s.print(m.label());
            s.print(' ');
        }
        s.println();
    }

    /**
     * Add the given task to our queue.
     * 
     * @param msg
     *            The task to add to the queue
     */
    int add(RunTaskMessage msg) {
        final int length;
        final TaskType type = msg.taskInstance.type;
        final WorkerQueueTaskInfo info = queueTypes[type.index];
        final int pos;
        synchronized (this) {
            if (activeTime == 0L) {
                activeTime = msg.arrivalMoment;
            }
            length = info.registerAdd();
            pos = findInsertionPoint(queue, msg);
            queue.add(pos, msg);
        }
        if (Settings.traceQueuing) {
            Globals.log.reportProgress("Adding "
                    + msg.taskInstance.formatJobAndType() + " at position "
                    + pos + " of worker queue; length is now " + queue.size()
                    + "; " + length + " of type " + type);
        }
        if (Settings.dumpWorkerQueue) {
            dumpQueue();
        }
        return length;
    }

    RunTaskMessage remove(Gossiper gossiper) {
        final RunTaskMessage res;
        final int length;
        final WorkerQueueTaskInfo info;

        synchronized (this) {
            if (queue.isEmpty()) {
                return null;
            }
            res = queue.remove(0);
            info = queueTypes[res.taskInstance.type.index];
            length = info.registerRemove();
        }
        if (Settings.traceQueuing) {
            Globals.log.reportProgress("Removing "
                    + res.taskInstance.formatJobAndType()
                    + " from worker queue; length is now " + queue.size()
                    + "; " + length + " of type " + res.taskInstance.type);
        }
        if (gossiper != null) {
            final long queueTimePerTask = info.getDequeueInterval();
            gossiper.setWorkerQueueTimePerTask(res.taskInstance.type,
                    queueTimePerTask, length);
        }
        return res;
    }

    boolean failTask(TaskType type) {
        final WorkerQueueTaskInfo info = queueTypes[type.index];
        info.failTask();

        // TODO: synchronize this properly; due to race conditions the last two
        // task types may be failed at the same time without either one
        // returning false.
        for (final WorkerQueueTaskInfo i : queueTypes) {
            if (i != null && !i.hasFailed()) {
                return false; // There still is a non-failed task type.
            }
        }
        return true; // All task types have failed.
    }

    long countTask(TaskType type, long computeInterval) {
        final WorkerQueueTaskInfo info = queueTypes[type.index];
        return info.countTask(computeInterval, type.unpredictable);
    }

    synchronized long getActiveTime(long startTime) {
        if (activeTime < startTime) {
            Globals.log.reportProgress("Worker was not used");
            return startTime;
        }
        return activeTime;
    }

    synchronized void printStatistics(PrintStream s, long workInterval) {
        for (final WorkerQueueTaskInfo t : queueTypes) {
            if (t != null) {
                t.printStatistics(s, workInterval);
            }
        }
    }

    WorkerQueueTaskInfo getTaskInfo(TaskType type) {
        return queueTypes[type.index];
    }

    void registerNode(NodeInfo nodeInfo) {
        for (final WorkerQueueTaskInfo info : queueTypes) {
            if (info != null) {
                info.registerNode(nodeInfo);
            }
        }
    }

    WorkerQueueTaskInfo getTaskInfo(int ix) {
        return queueTypes[ix];
    }

    synchronized void clear() {
        queue.clear();
    }
}
