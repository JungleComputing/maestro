package ibis.maestro;

import ibis.steel.ConstantEstimate;
import ibis.steel.Estimate;

import java.io.Serializable;

/**
 * A class containing information about the current worker queue length for the
 * given type.
 * 
 * @author Kees van Reeuwijk
 */
class WorkerQueueInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private int queueLength;

    private int queueLengthSequenceNumber;

    private Estimate dequeueTimePerJob;

    private Estimate executionTime;

    /**
     * @param queueLength
     *            The worker queue length.
     * @param queueLengthSequenceNumber
     *            The sequence number of this queue length. Used to avoid
     *            multiple updates to the worker allowance on multiple
     *            transmissions of the same WoekrQueueInfo instance.
     * @param zero
     *            The current wait time in the worker queue divided by the queue
     *            length.
     * @param executionTime
     *            The execution time of a job.
     */
    WorkerQueueInfo(final int queueLength, final int queueLengthSequenceNumber,
            final Estimate zero, final Estimate executionTime) {
        this.queueLength = queueLength;
        this.queueLengthSequenceNumber = queueLengthSequenceNumber;
        this.dequeueTimePerJob = zero;
        this.executionTime = executionTime;
    }

    /**
     * Returns a string representation of this completion info. (Overrides
     * method in superclass.)
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(ql=" + getQueueLength() + ",dq/t="
                + getDequeueTimePerJob().format() + ",compute="
                + getExecutionTime().format() + ")";
    }

    String format() {
        return String.format("%3d %9s %9s", getQueueLength(),
                Utils.formatSeconds(getDequeueTimePerJob().getAverage()),
                Utils.formatSeconds(getExecutionTime().getAverage()));
    }

    static String topLabel() {
        return String.format("%3s %9s %9s", "ql", "dequeue", "compute");
    }

    static String emptyFormat() {
        return String.format("%23s", "---");
    }

    static Object topLabelType(final JobType type) {
        return String.format("%23s", type.toString());
    }

    synchronized void failJob() {
        this.executionTime = null;
    }

    synchronized void setExecutionTime(final Estimate t) {
        this.executionTime = t;
    }

    synchronized void setQueueTimePerJob(final Estimate queueTimePerJob,
            final int newQueueLength) {
        this.dequeueTimePerJob = queueTimePerJob;
        if (this.queueLength != newQueueLength) {
            this.queueLength = newQueueLength;
            queueLengthSequenceNumber++;
        }
    }

    synchronized boolean setQueueLength(final int newQueueLength) {
        boolean changed = false;

        if (this.queueLength != newQueueLength) {
            this.queueLength = newQueueLength;
            queueLengthSequenceNumber++;
            changed = true;
        }
        return changed;
    }

    synchronized Estimate getExecutionTime() {
        return executionTime;
    }

    synchronized int getQueueLength() {
        return queueLength;
    }

    synchronized Estimate getDequeueTimePerJob() {
        if (Settings.IGNORE_QUEUE_TIME) {
            return ConstantEstimate.ZERO;
        }
        return dequeueTimePerJob;
    }

}
