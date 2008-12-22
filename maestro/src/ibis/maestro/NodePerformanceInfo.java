package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * A packet of node update info.
 * 
 * @author Kees van Reeuwijk.
 */
class NodePerformanceInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	/** The node this information is for. */
	final IbisIdentifier source;

	/**
	 * For each type of task we know, the estimated time it will take to
	 * complete the remaining tasks of this job.
	 */
	final long[] completionInfo;

	/** For each type of task we know, the queue length on this worker. */
	WorkerQueueInfo[] workerQueueInfo;

	long timeStamp;

	final int numberOfProcessors;

	NodePerformanceInfo(long[] completionInfo,
			WorkerQueueInfo[] workerQueueInfo, IbisIdentifier source,
			int numberOfProcessors, long timeStamp) {
		this.completionInfo = completionInfo;
		this.workerQueueInfo = workerQueueInfo;
		this.source = source;
		this.numberOfProcessors = numberOfProcessors;
		this.timeStamp = timeStamp;
	}

	NodePerformanceInfo getDeepCopy() {
		final long completionInfoCopy[] = Arrays.copyOf(completionInfo,
				completionInfo.length);
		final WorkerQueueInfo workerQueueInfoCopy[] = Arrays.copyOf(workerQueueInfo,
				workerQueueInfo.length);
		return new NodePerformanceInfo(completionInfoCopy, workerQueueInfoCopy,
				source, numberOfProcessors, timeStamp);
	}

	private String buildCompletionString() {
		final StringBuilder b = new StringBuilder("[");
		for (final long i : completionInfo) {
			b.append(Utils.formatNanoseconds(i));
		}
		b.append(']');
		return b.toString();
	}

	/**
	 * Returns a string representation of update message. (Overrides method in
	 * superclass.)
	 * 
	 * @return The string representation.
	 */
	@Override
	public String toString() {
		final String completion = buildCompletionString();
		final String workerQueue = Arrays.deepToString(workerQueueInfo);
		return "Update @" + timeStamp + " " + workerQueue + " " + completion;
	}

	long estimateJobCompletion(LocalNodeInfo localNodeInfo, TaskType type,
			boolean ignoreBusyProcessors) {
		if (localNodeInfo == null) {
			if (Settings.traceRemainingJobTime) {
				Globals.log.reportError("No local node info");
			}
			return Long.MAX_VALUE;
		}
		final WorkerQueueInfo queueInfo = workerQueueInfo[type.index];
		final long completionInterval = completionInfo[type.index];
		long unpredictableOverhead = 0L;

		if (queueInfo == null) {
			if (Settings.traceRemainingJobTime) {
				Globals.log.reportError("Node " + source
						+ " does not provide queue info for type " + type);
			}
			return Long.MAX_VALUE;
		}
		if (localNodeInfo.suspect) {
			if (Settings.traceRemainingJobTime) {
				Globals.log.reportError("Node " + source
						+ " is suspect, no completion estimate");
			}
			return Long.MAX_VALUE;
		}
		if (completionInterval == Long.MAX_VALUE) {
			if (Settings.traceRemainingJobTime) {
				Globals.log.reportError("Node " + source
						+ " has infinite completion time");
			}
			return Long.MAX_VALUE;
		}
		final int currentTasks = localNodeInfo.getCurrentTasks(type);
		if (type.unpredictable) {
			if (ignoreBusyProcessors && currentTasks >= numberOfProcessors) {
				// Don't submit jobs, there are no idle processors.
				if (Settings.traceRemainingJobTime) {
					Globals.log.reportError("Node " + source
							+ " has no idle processors");
				}
				return Long.MAX_VALUE;
			}
			// The compute time is just based on an initial estimate. Give nodes
			// already running tasks some penalty to encourage spreading the
			// load
			// over nodes.
			unpredictableOverhead = (currentTasks * queueInfo.executionTime) / 10;
		} else {
			final int allowance = localNodeInfo.getAllowance(type);
			if (ignoreBusyProcessors && currentTasks >= allowance) {
				if (Settings.traceRemainingJobTime) {
					Globals.log
					.reportError("Node "
							+ source
							+ " uses its allowance, no completion estimate: currentTasks="
							+ currentTasks + " allowance=" + allowance);
				}
				return Long.MAX_VALUE;
			}
		}
		// FIXME: handle this hard-coded pessimistic estimate of transmission
		// time more robustly.
		final long transmissionTime = 3*localNodeInfo.getTransmissionTime(type);
		final int extra = (currentTasks >= numberOfProcessors) ? 1 : 0;
		final long total = Utils.safeAdd(transmissionTime,
				(extra + queueInfo.queueLength) * queueInfo.dequeueTimePerTask,
				queueInfo.executionTime, completionInterval,
				unpredictableOverhead);
		if (Settings.traceRemainingJobTime) {
			Globals.log.reportProgress("Estimated completion time for "
					+ source + " is " + Utils.formatNanoseconds(total));
		}
		return total;
	}

	/**
	 * Given the index of a type, return the interval in nanoseconds it will
	 * take from the moment a task of this type arrives at the worker queue of
	 * this node, until the entire job it belongs to is completed.
	 * 
	 * @param ix
	 *            The index of the type we're interested in.
	 */
	long getCompletionOnWorker(int ix, int nextIx) {
		final WorkerQueueInfo info = workerQueueInfo[ix];
		long nextCompletionInterval;

		if (info == null) {
			// We don't support this type.
			return Long.MAX_VALUE;
		}
		if (nextIx >= 0) {
			nextCompletionInterval = completionInfo[nextIx];
		} else {
			nextCompletionInterval = 0L;
		}
		return Utils.safeAdd((1 + info.queueLength) * info.dequeueTimePerTask,
				info.executionTime, nextCompletionInterval);
	}

	void print(PrintStream s) {
		for (final WorkerQueueInfo i : workerQueueInfo) {
			if (i == null) {
				s.print(WorkerQueueInfo.emptyFormat());
			} else {
				s.print(i.format());
				s.print(' ');
			}
		}
		s.print(" | ");
		for (final long t : completionInfo) {
			s.printf("%8s ", Utils.formatNanoseconds(t));
		}
		s.println(source);
	}

	static void printTopLabel(PrintStream s) {
		for (final TaskType t : Globals.allTaskTypes) {
			s.print(WorkerQueueInfo.topLabelType(t));
			s.print(' ');
		}
		s.print(" | ");
		for (final TaskType t : Globals.allTaskTypes) {
			s.printf("%8s ", t);
		}
		s.println();
		for (@SuppressWarnings("unused") final
				TaskType t : Globals.allTaskTypes) {
			s.print(WorkerQueueInfo.topLabel());
			s.print(' ');
		}
		s.print(" | ");
		s.println();
	}

	void failTask(TaskType type) {
		final WorkerQueueInfo info = workerQueueInfo[type.index];
		if (info != null) {
			info.failTask();
			timeStamp = System.nanoTime();
		}
	}

	void setComputeTime(TaskType type, long t) {
		final WorkerQueueInfo info = workerQueueInfo[type.index];
		if (info != null) {
			info.setComputeTime(t);
			timeStamp = System.nanoTime();
		}
	}

	void setQueueTimePerTask(TaskType type, long queueTimePerTask,
			int queueLength) {
		final WorkerQueueInfo info = workerQueueInfo[type.index];
		if (info != null) {
			info.setQueueTimePerTask(queueTimePerTask, queueLength);
			timeStamp = System.nanoTime();
		}
	}

	void setQueueLength(TaskType type, int newQueueLength) {
		final WorkerQueueInfo info = workerQueueInfo[type.index];
		if (info != null) {
			info.setQueueLength(newQueueLength);
			timeStamp = System.nanoTime();
		}
	}
}
