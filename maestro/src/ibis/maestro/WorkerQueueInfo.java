package ibis.maestro;

import java.io.Serializable;

/**
 * A class representing the current worker queue length for the given type.
 * 
 * @author Kees van Reeuwijk
 */
class WorkerQueueInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private int queueLength;

    private int queueLengthSequenceNumber;

    private double dequeueTimePerJob;

    private double executionTime;

    /**
     * @param queueLength
     *            The worker queue length.
     * @param queueLengthSequenceNumber
     *            The sequence number of this queue length. Used to avoid
     *            multiple updates to the worker allowance on multiple
     *            transmissions of the same WoekrQueueInfo instance.
     * @param dequeueTimePerJob
     *            The current wait time in the worker queue divided by the queue
     *            length.
     * @param executionTime
     *            The execution time of a job.
     */
    WorkerQueueInfo(int queueLength, int queueLengthSequenceNumber,
            double dequeueTimePerJob, double executionTime) {
        this.queueLength = queueLength;
        this.queueLengthSequenceNumber = queueLengthSequenceNumber;
        this.dequeueTimePerJob = dequeueTimePerJob;
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
                + Utils.formatSeconds(getDequeueTimePerJob())
                + ",compute=" + Utils.formatSeconds(getExecutionTime())
                + ")";
    }

    String format() {
        return String.format("%3d %9s %9s", getQueueLength(), Utils
                .formatSeconds(getDequeueTimePerJob()), Utils
                .formatSeconds(getExecutionTime()));
    }

    static String topLabel() {
        return String.format("%3s %9s %9s", "ql", "dequeue", "compute");
    }

    static String emptyFormat() {
        return String.format("%23s", "---");
    }

    static Object topLabelType(JobType type) {
        return String.format("%23s", type.toString());
    }

    synchronized void failJob() {
        this.executionTime = Double.POSITIVE_INFINITY;
    }

    synchronized void setExecutionTime(double t) {
        this.executionTime = t;
    }

    synchronized void setQueueTimePerJob(double queueTimePerJob,
            int newQueueLength) {
        this.dequeueTimePerJob = queueTimePerJob;
        if (this.queueLength != newQueueLength) {
            this.queueLength = newQueueLength;
            queueLengthSequenceNumber++;
        }
    }

    synchronized void setQueueLength(int newQueueLength) {
        if (this.queueLength != newQueueLength) {
            this.queueLength = newQueueLength;
            queueLengthSequenceNumber++;
        }
    }

    synchronized double getExecutionTime() {
        return executionTime;
    }

    synchronized int getQueueLength() {
        return queueLength;
    }

    synchronized double getDequeueTimePerJob() {
        if (Settings.IGNORE_QUEUE_TIME) {
            return 0L;
        }
        return dequeueTimePerJob;
    }

}
