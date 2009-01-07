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
	private long dequeueTimePerTask;
	private long executionTime;

	/**
	 * @param queueLength
	 *            The worker queue length.
	 * @param queueLengthSequenceNumber
	 *            The sequence number of this queue length. Used to avoid
	 *            multiple updates to the worker allowance on multiple
	 *            transmissions of the same WoekrQueueInfo instance.
	 * @param dequeueTimePerTask
	 *            The current wait time in the worker queue divided by the queue
	 *            length.
	 * @param executionTime
	 *            The execution time of a task.
	 */
	WorkerQueueInfo(int queueLength, int queueLengthSequenceNumber,
			long dequeueTimePerTask, long executionTime) {
		this.queueLength = queueLength;
		this.queueLengthSequenceNumber = queueLengthSequenceNumber;
		this.dequeueTimePerTask = dequeueTimePerTask;
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
		+ Utils.formatNanoseconds(getDequeueTimePerTask()) + ",compute="
		+ Utils.formatNanoseconds(getExecutionTime()) + ")";
	}

	String format() {
		return String.format("%3d %9s %9s", getQueueLength(), Utils
				.formatNanoseconds(getDequeueTimePerTask()), Utils
				.formatNanoseconds(getExecutionTime()));
	}

	static String topLabel() {
		return String.format("%3s %9s %9s", "ql", "dequeue", "compute");
	}

	static String emptyFormat() {
		return String.format("%23s", "---");
	}

	static Object topLabelType(TaskType type) {
		return String.format("%23s", type.toString());
	}

	synchronized void failTask() {
		this.executionTime = Long.MAX_VALUE;
	}

	synchronized void setExecutionTime(long t) {
		this.executionTime = t;
	}

	synchronized void setQueueTimePerTask(long queueTimePerTask,
			int newQueueLength) {
		this.dequeueTimePerTask = queueTimePerTask;
		if( this.queueLength != newQueueLength ){
			this.queueLength = newQueueLength;
			queueLengthSequenceNumber++;
		}
	}

	synchronized void setQueueLength(int newQueueLength) {
		if( this.queueLength != newQueueLength){
			this.queueLength = newQueueLength;
			queueLengthSequenceNumber++;
		}
	}

	synchronized long getExecutionTime() {
		return executionTime;
	}

	synchronized int getQueueLength() {
		return queueLength;
	}

	synchronized long getDequeueTimePerTask() {
		if( Settings.IGNORE_QUEUE_TIME ){
			return 0L;
		}
		return dequeueTimePerTask;
	}

	synchronized int getQueueLengthSequenceNumber() {
		return queueLengthSequenceNumber;
	}

}
