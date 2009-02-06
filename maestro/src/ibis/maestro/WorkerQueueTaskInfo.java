package ibis.maestro;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Statistics per type for the different task types in the queue.
 * 
 * @author Kees van Reeuwijk
 */
final class WorkerQueueTaskInfo {
    /** The type these statistics are about. */
    final TaskType type;

    /** The workers that are willing to execute this task. */
    private final List<NodeTaskInfo> workers = new LinkedList<NodeTaskInfo>();

    /** The total number of tasks of this type that entered the queue. */
    private long incomingTaskCount = 0;

    private int outGoingTaskCount = 0;

    /** Current number of elements of this type in the queue. */
    private int elements = 0;

    private int sequenceNumber = 0;

    /** Maximal ever number of elements in the queue. */
    private int maxElements = 0;

    /** The last moment in s that the front of the queue changed. */
    private double frontChangedTime = 0;

    private boolean failed = false;

    /** The estimated time interval between tasks being dequeued. */
    private final TimeEstimate dequeueInterval = new TimeEstimate(
            1 * Utils.MILLISECOND);

    private long totalWorkTime = 0;

    private final TimeEstimate averageComputeTime = new TimeEstimate(
            Utils.MILLISECOND);

    WorkerQueueTaskInfo(TaskType type) {
        this.type = type;
    }

    synchronized void printStatistics(PrintStream s, double workTime) {
        s.println("worker queue for " + type + ": " + incomingTaskCount
                + " tasks; dequeue interval: " + dequeueInterval
                + "; maximal queue size: " + maxElements);
        final double workPercentage = 100.0 * (totalWorkTime / workTime);
        final PrintStream out = s;
        if (outGoingTaskCount > 0) {
            out.println("Worker: " + type + ":");
            out.printf("    # tasks          = %5d\n", outGoingTaskCount);
            out.println("    total work time      = "
                    + Utils.formatSeconds(totalWorkTime)
                    + String.format(" (%.1f%%)", workPercentage));
            out.println("    work time/task       = "
                    + Utils
                            .formatSeconds(totalWorkTime
                                    / outGoingTaskCount));
            out.println("    av. dequeue interval = "
                    + Utils.formatSeconds(dequeueInterval.getAverage()));
        } else {
            out.println("Worker: " + type + " is unused");
        }
    }

    int registerAdd() {
        elements++;
        if (elements > maxElements) {
            maxElements = elements;
        }
        if (frontChangedTime == 0) {
            // This entry is the front of the queue,
            // record the time it became this.
            frontChangedTime = Utils.getPreciseTime();
        }
        incomingTaskCount++;
        sequenceNumber++;
        return elements;
    }

    int registerRemove() {
        final double now = Utils.getPreciseTime();
        if (frontChangedTime != 0) {
            // We know when this entry became the front of the queue.
            final double i = now - frontChangedTime;
            dequeueInterval.addSample(i);
        }
        elements--;
        sequenceNumber++;
        if (elements < 0) {
            Globals.log
                    .reportInternalError("Number of elements in worker queue is now negative?? type="
                            + type + " elements=" + elements);
        }
        if (elements == 0) {
            // Don't take the next dequeuing into account,
            // since the queue is now empty.
            frontChangedTime = 0l;
        } else {
            frontChangedTime = now;
        }
        return elements;
    }

    double getDequeueInterval() {
        return dequeueInterval.getAverage();
    }

    /**
     * Registers the completion of a task of this particular type, with the
     * given queue interval and the given work interval.
     * 
     * @param workTime
     *            The time it took to execute this task.
     */
    synchronized double countTask(double workTime, boolean unpredictable) {
        outGoingTaskCount++;
        totalWorkTime += workTime;
        if (!unpredictable) {
            averageComputeTime.addSample(workTime);
        }
        return averageComputeTime.getAverage();
    }

    /**
     * Registers that this node can no longer execute this type of task.
     */
    synchronized void failTask() {
        failed = true;
    }

    synchronized boolean hasFailed() {
        return failed;
    }

    /**
     * Sets the initial compute time estimate of this task to the given value.
     * 
     * @param estimate
     *            The initial estimate.
     */
    void setInitialComputeTimeEstimate(double estimate) {
        averageComputeTime.setInitialEstimate(estimate);
    }

    void registerNode(NodeInfo nodeInfo) {
        final NodeTaskInfo nodeTaskInfo = nodeInfo.get(type);
        synchronized (this) {
            if (nodeTaskInfo != null) {
                workers.add(nodeTaskInfo);
            }
        }
    }
}