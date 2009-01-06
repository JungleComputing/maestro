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
	private long queueTime;
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
	WorkerQueueInfo(int queueLength, int queueLengthSequenceNumber
			, long queueTime, long executionTime) {
		this.queueLength = queueLength;
		this.queueLengthSequenceNumber = queueLengthSequenceNumber;
		this.queueTime = queueTime;
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
		return "(ql=" + getQueueLength() + ",qt="
		+ Utils.formatNanoseconds(getQueueTime()) + ",compute="
		+ Utils.formatNanoseconds(getExecutionTime()) + ")";
	}

	String format() {
		return String.format("%3d %9s %9s", getQueueLength(), Utils
				.formatNanoseconds(getQueueTime()), Utils
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

	void failTask() {
		this.executionTime = Long.MAX_VALUE;
	}

	void setExecutionTime(long t) {
		this.executionTime = t;
	}

	synchronized void setQueueTime(
			int newQueueLength, long queueTime) {
		this.queueTime = queueTime;
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

	long getExecutionTime() {
		return executionTime;
	}

	synchronized int getQueueLength() {
		return queueLength;
	}

	long getQueueTime(){
		return queueTime;
	}

	int getQueueLengthSequenceNumber() {
		return queueLengthSequenceNumber;
	}

}
