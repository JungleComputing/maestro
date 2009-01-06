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
		TaskType[] taskTypes = Globals.allTaskTypes;
		queueTypes = new WorkerQueueTaskInfo[taskTypes.length];
		for (TaskType t : taskTypes) {
			WorkerQueueTaskInfo queueTypeInfo = new WorkerQueueTaskInfo(t);
			queueTypes[t.index] = queueTypeInfo;
			Task task = jobs.getTask(t);
			if (task instanceof TaskExecutionTimeEstimator) {
				TaskExecutionTimeEstimator estimator = (TaskExecutionTimeEstimator) task;
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
		long id = msg.taskInstance.jobInstance.id;
		while (true) {
			int mid = (start + end) / 2;
			if (mid == start) {
				break;
			}
			long midId = queue.get(mid).taskInstance.jobInstance.id;
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
		long startId = queue.get(start).taskInstance.jobInstance.id;
		if (startId < id) {
			return end;
		}
		return start;
	}

	private void dumpQueue() {
		Globals.log.reportProgress("Worker queue: ");
		PrintStream s = Globals.log.getPrintStream();
		for (RunTaskMessage m : queue) {
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
	synchronized int add(RunTaskMessage msg) {
		if (activeTime == 0L) {
			activeTime = msg.arrivalMoment;
		}
		TaskType type = msg.taskInstance.type;
		WorkerQueueTaskInfo info = queueTypes[type.index];
		int length = info.registerAdd();
		int pos = findInsertionPoint(queue, msg);
		queue.add(pos, msg);
		if (Settings.traceQueuing) {
			Globals.log.reportProgress("Adding "
					+ msg.taskInstance.formatJobAndType() + " at position "
					+ pos + " of worker queue; length is now " + queue.size()
					+ "; " + length + " of type " + type);
		}
		msg.setQueueMoment(msg.arrivalMoment, length);
		if (Settings.dumpWorkerQueue) {
			dumpQueue();
		}
		return length;
	}

	boolean failTask(TaskType type) {
		WorkerQueueTaskInfo info = queueTypes[type.index];
		info.failTask();

		// TODO: synchronize this properly; due to race conditions the last two
		// task types may be failed at the same time without either one
		// returning false.
		for (WorkerQueueTaskInfo i : queueTypes) {
			if (i != null && !i.hasFailed()) {
				return false; // There still is a non-failed task type.
			}
		}
		return true; // All task types have failed.
	}

	long countTask(TaskType type, long computeInterval) {
		WorkerQueueTaskInfo info = queueTypes[type.index];
		return info.countTask(computeInterval, type.unpredictable);
	}

	synchronized RunTaskMessage remove(Gossiper gossiper) {
		if (queue.isEmpty()) {
			return null;
		}
		RunTaskMessage res = queue.remove(0);
		WorkerQueueTaskInfo info = queueTypes[res.taskInstance.type.index];
		int length = info.registerRemove();
		if (Settings.traceQueuing) {
			Globals.log.reportProgress("Removing "
					+ res.taskInstance.formatJobAndType()
					+ " from worker queue; length is now " + queue.size()
					+ "; " + length + " of type " + res.taskInstance.type);
		}
		long queueInterval = System.nanoTime() - res.getQueueMoment();
		if (gossiper != null) {
			gossiper.setQueueTimePerTask(res.taskInstance.type, queueInterval
					/ res.getQueueLength(), length);
		}
		return res;
	}

	synchronized long getActiveTime(long startTime) {
		if (activeTime < startTime) {
			Globals.log.reportProgress("Worker was not used");
			return startTime;
		}
		return activeTime;
	}

	synchronized void printStatistics(PrintStream s, long workInterval) {
		for (WorkerQueueTaskInfo t : queueTypes) {
			if (t != null) {
				t.printStatistics(s, workInterval);
			}
		}
	}

	WorkerQueueTaskInfo getTaskInfo(TaskType type) {
		return queueTypes[type.index];
	}

	void registerNode(NodeInfo nodeInfo) {
		for (WorkerQueueTaskInfo info : queueTypes) {
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
