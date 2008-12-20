package ibis.maestro;

import java.io.PrintStream;

/**
 * Information the node has about a particular task type on a particular node.
 */
final class NodeTaskInfo {
	final WorkerQueueTaskInfo taskInfo;

	private final NodeInfo nodeInfo;

	private final TimeEstimate transmissionTimeEstimate;

	private final TimeEstimate roundtripTimeEstimate;

	/** How many instances of this task does this worker currently have? */
	private int outstandingTasks = 0;

	/** How many task instances has this worker executed until now? */
	private int executedTasks = 0;

	/** The maximal ever allowance given to this worker for this task. */
	private int maximalEverAllowance;

	/**
	 * How many outstanding instances of this task should this worker maximally
	 * have?
	 */
	private int allowance;

	private int allowanceSequenceNumber = -1;

	private boolean failed = false;

	private Counter missedAllowanceDeadlines = new Counter();
	private Counter missedRescheduleDeadlines = new Counter();

	/**
	 * Constructs a new information class for a particular task type for a
	 * particular worker.
	 * 
	 * @param taskInfo
	 *            The type of task we have administration for.
	 * @param worker
	 *            The worker we have administration for.
	 * @param local
	 *            True iff this is the local worker.
	 * @param unpredictable
	 *            True iff the execution time of this task is unpredictable.
	 * @param pingTime
	 *            The ping time of this worker.
	 */
	NodeTaskInfo(WorkerQueueTaskInfo taskInfo, NodeInfo worker, boolean local,
			boolean unpredictable, long pingTime) {
		this.taskInfo = taskInfo;
		this.nodeInfo = worker;
		this.allowance = (unpredictable || !local) ? 0 : 1;
		this.maximalEverAllowance = allowance;

		// Totally unfounded guesses, but we should learn soon enough what the
		// real values are...
		this.transmissionTimeEstimate = new TimeEstimate(pingTime);
		this.roundtripTimeEstimate = new TimeEstimate(2 * pingTime);
		if (Settings.traceWorkerList || Settings.traceRemainingJobTime) {
			Globals.log.reportProgress("Created new WorkerTaskInfo "
					+ toString());
		}
	}

	/**
	 * @return A string representation of this class instance.
	 */
	@Override
	public String toString() {
		return "[taskInfo=" + taskInfo + " worker=" + nodeInfo
				+ " transmissionTimeEstimate=" + transmissionTimeEstimate
				+ " outstandingTasks=" + outstandingTasks
				+ " maximalAllowance=" + allowance + "]";
	}

	/**
	 * Registers the completion of a task.
	 * 
	 * @param roundtripTime
	 *            The total roundtrip time of this task.
	 */
	synchronized void registerTaskCompleted(long roundtripTime) {
		executedTasks++;
		outstandingTasks--;
		roundtripTimeEstimate.addSample(roundtripTime);
		if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
			String label = "task=" + taskInfo + " worker=" + nodeInfo;
			Globals.log.reportProgress(label + ": roundTripTimeEstimate="
					+ roundtripTimeEstimate);
		}
	}

	/**
	 * Registers the reception of a task by the worker.
	 * 
	 * @param transmissionTime
	 *            The transmission time of this task.
	 */
	synchronized void registerTaskReceived(long transmissionTime) {
		transmissionTimeEstimate.addSample(transmissionTime);
		if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
			String label = "task=" + taskInfo + " worker=" + nodeInfo;
			Globals.log.reportProgress(label + ": transmissionTimeEstimate="
					+ roundtripTimeEstimate);
		}
	}

	synchronized void registerTaskFailed() {
		allowance = 0;
		failed = true;
	}

	void registerMissedAllowanceDeadline() {
		missedAllowanceDeadlines.add();
	}

	void registerMissedRescheduleDeadline() {
		missedRescheduleDeadlines.add();
	}

	/**
	 * Update the roundtrip time estimate with the given value. (Used by the
	 * handling of missed deadlines.
	 * 
	 * @param t
	 *            The new estimate of the roundtrip time.
	 */
	synchronized void updateRoundtripTimeEstimate(long t) {
		roundtripTimeEstimate.addSample(t);
	}

	/** Register that there is a new outstanding task. */
	synchronized void incrementOutstandingTasks() {
		outstandingTasks++;
	}

	/**
	 * @return True iff this worker ever executed a task of this type.
	 */
	private synchronized boolean didWork() {
		return (executedTasks != 0) || (outstandingTasks != 0);
	}

	/**
	 * Given a queue length on the worker, manipulate the allowance to ensure
	 * the queue lengths stays within very reasonable limits.
	 * 
	 * @param queueLength
	 *            The worker queue length.
	 * @param sequenceNumber
	 *            The sequence number of this queue length.
	 */
	synchronized boolean controlAllowance(int queueLength, int sequenceNumber) {
		boolean changed = false;

		if (failed) {
			allowance = 0;
			return false;
		}
		if (allowanceSequenceNumber < sequenceNumber
				&& allowance == outstandingTasks) {
			// We can only regulate the allowance if we are
			// at our current maximal allowance.
			// Also, we should only regulate on a more recent sequence number
			// than we already have.
			int oldAllowance = allowance;

			allowanceSequenceNumber = sequenceNumber;
			if (queueLength < 1) {
				allowance++;
			} else if (queueLength > 4) {
				// There are a lot of items in the queue; take a larger step.
				allowance -= 2;
			} else if (queueLength > 1) {
				allowance--;
			}
			if (allowance < 0) {
				// Yes, we are prepared to cut off a worker entirely.
				// However, if a worker reports it has room in its queue
				// we will increase its allowance again.
				allowance = 0;
			}
			if (allowance > 15) {
				// We arbitrarily limit the maximal allowance since larger
				// than that doesn't seem useful.
				// Hitting this limit is a strong indication that something
				// is wrong, though.
				allowance = 15;
			}
			if (maximalEverAllowance < allowance) {
				maximalEverAllowance = allowance;
			}
			if (Settings.traceAllowance) {
				Globals.log.reportProgress("controlAllowance(): task="
						+ taskInfo.type + " node=" + nodeInfo + " queueLength="
						+ queueLength + " allowance=" + oldAllowance + "->"
						+ allowance);
			}
			changed = (allowance != oldAllowance);
		}
		return changed;
	}

	synchronized long estimateRoundtripTime() {
		if (failed) {
			return Long.MAX_VALUE;
		}
		return roundtripTimeEstimate.getAverage();
	}

	synchronized void printStatistics(PrintStream s) {
		if (didWork()) {
			s.println("  " + taskInfo.type + ": executed " + executedTasks
					+ " tasks; maximal allowance " + maximalEverAllowance
					+ ", xmit time " + transmissionTimeEstimate
					+ (failed ? " FAILED" : ""));
			int missedAllowance = missedAllowanceDeadlines.get();
			int missedReschedule = missedRescheduleDeadlines.get();
			if (missedAllowance > 0 || missedReschedule > 0) {
				s.println("  " + taskInfo.type
						+ ": missed deadlines: allowance: " + missedAllowance
						+ " reschedule: " + missedReschedule);
			}
		}
	}

	synchronized int getCurrentTasks() {
		return outstandingTasks;
	}

	synchronized long getTransmissionTime() {
		return transmissionTimeEstimate.getAverage();
	}

	synchronized int getAllowance() {
		return allowance;
	}

	synchronized boolean isAvailable() {
		return outstandingTasks < allowance;
	}
}
