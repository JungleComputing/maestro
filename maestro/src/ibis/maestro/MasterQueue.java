package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;

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
final class MasterQueue {
	private int taskCount = 0;
	private final TypeInfo queueTypes[];
	private final ArrayList<TaskInstance> queue = new ArrayList<TaskInstance>();

	/**
	 * Statistics per type for the different task types in the queue.
	 * 
	 * @author Kees van Reeuwijk
	 */
	private static final class TypeInfo {
		/** The type these statistics are about. */
		private final TaskType type;

		/** The total number of tasks of this type that entered the queue. */
		private long taskCount = 0;

		/** Current number of elements of this type in the queue. */
		private int elements = 0;

		/** Maximal ever number of elements in the queue. */
		private int maxElements = 0;

		private long frontChangedTime = 0;

		/** The estimated time interval between tasks being dequeued. */
		private final TimeEstimate dequeueInterval = new TimeEstimate(
				1 * Utils.MILLISECOND_IN_NANOSECONDS);

		TypeInfo(final TaskType type) {
			this.type = type;
		}

		private synchronized void printStatistics(PrintStream s) {
			s.println("master queue for " + type + ": " + taskCount
					+ " tasks; dequeue interval: " + dequeueInterval
					+ "; maximal queue size: " + maxElements);
		}

		synchronized private int registerAdd() {
			elements++;
			if (elements > maxElements) {
				maxElements = elements;
			}
			if (frontChangedTime == 0) {
				// This entry is the front of the queue,
				// record the time it became this.
				frontChangedTime = System.nanoTime();
			}
			taskCount++;
			return elements;
		}

		synchronized void registerRemove() {
			final long now = System.nanoTime();
			if (frontChangedTime != 0) {
				// We know when this entry became the front of the queue.
				final long i = now - frontChangedTime;
				dequeueInterval.addSample(i);
			}
			elements--;
			if (elements == 0) {
				// Don't take the next dequeuing into account,
				// since the queue is now empty.
				frontChangedTime = 0l;
			} else {
				frontChangedTime = now;
			}
		}

		/**
		 * Estimate the time a new task will spend in the queue.
		 * 
		 * @return The estimated time in nanoseconds a new task will spend in
		 *         the queue.
		 */
		synchronized long estimateQueueTime() {
			final long timePerEntry = dequeueInterval.getAverage();
			// Since at least one processor isn't working on a task (or we
			// wouldn't be here), we are only impressed if there is more
			// than one idle processor.
			final long res = timePerEntry * (1 + elements);
			return res;
		}

	}

	/**
	 * Constructs a new MasterQueue.
	 * 
	 * @param taskTypes
	 *            The supported types.
	 */
	MasterQueue(TaskType allTypes[]) {
		queueTypes = new TypeInfo[allTypes.length];
		for (final TaskType type : allTypes) {
			queueTypes[type.index] = new TypeInfo(type);
		}
	}

	private static int findInsertionPoint(ArrayList<TaskInstance> queue,
			TaskInstance e) {
		// Good old binary search.
		int start = 0;
		int end = queue.size();
		if (end == 0) {
			// The queue is empty. This is the only case where start
			// points to a non-existent element, so we have to treat
			// it separately.
			return 0;
		}
		final long id = e.jobInstance.id;
		while (true) {
			final int mid = (start + end) / 2;
			if (mid == start) {
				break;
			}
			final long midId = queue.get(mid).jobInstance.id;
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
		final long startId = queue.get(start).jobInstance.id;
		if (startId < id) {
			return end;
		}
		return start;
	}

	/**
	 * Returns true iff the entire queue is empty.
	 * 
	 * @return
	 */
	synchronized boolean isEmpty() {
		return queue.isEmpty();
	}

	private synchronized void dumpQueue(PrintStream s) {
		for (final TaskInstance e : queue) {
			s.print(e.shortLabel());
			s.print(' ');
		}
		s.println();
	}

	/**
	 * Submit a new task, belonging to the job with the given identifier, to the
	 * queue.
	 * 
	 * @param task
	 *            The task to submit.
	 */
	@SuppressWarnings("synthetic-access")
	protected synchronized void add(TaskInstance task) {
		taskCount++;
		final int pos = findInsertionPoint(queue, task);
		queue.add(pos, task);

		final TaskType type = task.type;
		final TypeInfo info = queueTypes[type.index];
		final int length = info.registerAdd();
		if (Settings.traceQueuing) {
			Globals.log.reportProgress("Adding " + task.formatJobAndType()
					+ " at position " + pos
					+ " of master queue; length is now " + queue.size() + "; "
					+ length + " of type " + type);
		}
		if (Settings.dumpMasterQueue) {
			dumpQueue(Globals.log.getPrintStream());
		}
	}

	/**
	 * Adds all the task instances in the given list to the queue. The list may
	 * be <code>null</code>.
	 * 
	 * @param l
	 *            The list of task instances to add.
	 */
	void add(ArrayList<TaskInstance> l) {
		if( l != null ){
			for (final TaskInstance task : l) {
				add(task);
			}
		}
	}

	synchronized TaskInstance remove() {
		return queue.remove(0);
	}

	@SuppressWarnings("synthetic-access")
	void printStatistics(PrintStream s) {
		for (final TypeInfo t : queueTypes) {
			if (t != null) {
				t.printStatistics(s);
			}
		}
		s.printf("Master: # incoming tasks = %5d\n", taskCount);
	}

	/**
	 * Given a task type, select the best worker from the list that has a free
	 * slot. In this context 'best' is simply the worker with the shortest
	 * overall completion time.
	 * 
	 * @param type
	 *            The type of task we want to execute.
	 * @return The info of the best worker for this task, or <code>null</code>
	 *         if there currently aren't any workers for this task type.
	 */
	private Submission selectBestWorker(
			HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap,
			NodePerformanceInfo tables[], TaskInstance task) {
		NodePerformanceInfo best = null;
		long bestInterval = Long.MAX_VALUE;

		for (final NodePerformanceInfo info : tables) {
			final LocalNodeInfo localNodeInfo = localNodeInfoMap
			.get(info.source);
			final long val = info.estimateJobCompletion(localNodeInfo,
					task.type, Settings.HARD_ALLOWANCES );

			if (val < bestInterval) {
				bestInterval = val;
				best = info;
			}
		}
		if (Settings.traceWorkerSelection) {
			final PrintStream s = Globals.log.getPrintStream();
			for (final NodePerformanceInfo i : tables) {
				i.print(s);
			}
			s.print("Best worker: ");
			for (final NodePerformanceInfo info : tables) {
				final LocalNodeInfo localNodeInfo = localNodeInfoMap
				.get(info.source);
				final long val = info.estimateJobCompletion(localNodeInfo,
						task.type, true);
				s.print(Utils.formatNanoseconds(val));
				if (val == bestInterval && val != Long.MAX_VALUE) {
					s.print('$');
				}
				s.print(' ');
			}
			s.println();
		}
		if (best == null) {
			if (Settings.traceMasterQueue) {
				Globals.log.reportProgress("No workers for task of type "
						+ task.type);
			}
			return null;
		}
		if (Settings.traceMasterQueue) {
			Globals.log.reportProgress("Selected worker " + best.source
					+ " for task of type " + task.type);
		}
		final LocalNodeInfo localNodeInfo = localNodeInfoMap.get(best.source);
		final long predictedDuration = localNodeInfo
		.getPredictedDuration(task.type);
		return new Submission(task, best.source, predictedDuration);
	}

	/**
	 * Given a task type, select the best worker from the list that has a free
	 * slot. In this context 'best' is simply the worker with the shortest
	 * overall completion time.
	 * 
	 * @param type
	 *            The type of task we want to execute.
	 * @return The info of the best worker for this task, or <code>null</code>
	 *         if there currently aren't any workers for this task type.
	 */
	private Submission selectBestWorker(
			HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap,
			AntRoutingTable table, TaskInstance task) {
		final NodePerformanceInfo best = null;

		final NodeInfo candidate = table.getBestReadyWorker(task.type);
		if (candidate == null) {
			if (Settings.traceMasterQueue) {
				Globals.log.reportProgress("No workers for task of type "
						+ task.type);
			}
			return null;
		}
		if (Settings.traceMasterQueue) {
			Globals.log.reportProgress("Selected worker " + best.source
					+ " for task of type " + task.type);
		}
		final LocalNodeInfo localNodeInfo = localNodeInfoMap
		.get(candidate.ibis);
		final long predictedDuration = localNodeInfo
		.getPredictedDuration(task.type);
		return new Submission(task, candidate.ibis, predictedDuration);
	}

	/**
	 * Get a job submission from the queue. The entries are tried from front to
	 * back, and the first one for which a worker can be found is returned.
	 * 
	 * @param tables
	 *            Timing tables for the different workers.
	 * @return A job submission, or <code>null</code> if there are no free
	 *         workers for any of the jobs in the queue.
	 */
	synchronized Submission getSubmission(
			HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap,
			NodePerformanceInfo[] tables) {
		int busyTypeIndex = -1; // Don't even consider tasks of this type, all
		// workers are busy.
		int ix = 0;
		while (ix < queue.size()) {
			final TaskInstance task = queue.get(ix);
			final TaskType type = task.type;
			if (type.index == busyTypeIndex) {
				if (Settings.traceMasterQueue || Settings.traceQueuing) {
					Globals.log.reportProgress("Type " + type
							+ " has no ready workers, don't bother");
				}
			} else {
				final Submission sub = selectBestWorker(localNodeInfoMap,
						tables, task);
				if (sub == null) {
					busyTypeIndex = type.index;
				} else {
					queue.remove(ix);
					final TypeInfo queueTypeInfo = queueTypes[type.index];
					queueTypeInfo.registerRemove();
					if (Settings.traceMasterQueue || Settings.traceQueuing) {
						final int length = queueTypeInfo.elements;
						Globals.log.reportProgress("Removing "
								+ task.formatJobAndType()
								+ " from master queue; length is now "
								+ queue.size() + "; " + length + " of type "
								+ type);
					}
					return sub;
				}
				if (Settings.traceMasterQueue) {
					Globals.log.reportProgress("No ready worker for task type "
							+ type);
				}
			}
			ix++;
		}
		return null;
	}

	/**
	 * @param antRoutingTable
	 * @return A task to submit.
	 */
	synchronized Submission getAntSubmission(
			HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap,
			AntRoutingTable antRoutingTable) {
		int ix = 0;
		int busyTypeIndex = -1; // No workers for this type.

		while (ix < queue.size()) {
			final TaskInstance task = queue.get(ix);
			final TaskType type = task.type;

			if (type.index == busyTypeIndex) {
				if (Settings.traceMasterQueue || Settings.traceQueuing) {
					Globals.log.reportProgress("Type " + type
							+ " has no ready workers, don't bother");
				}
			} else {
				final Submission sub = selectBestWorker(localNodeInfoMap,
						antRoutingTable, task);
				if (sub == null) {
					busyTypeIndex = type.index;
				} else {
					queue.remove(ix);
					final TypeInfo queueTypeInfo = queueTypes[type.index];
					queueTypeInfo.registerRemove();
					if (Settings.traceMasterQueue || Settings.traceQueuing) {
						final int length = queueTypeInfo.elements;
						Globals.log.reportProgress("Removing "
								+ task.formatJobAndType()
								+ " from master queue; length is now "
								+ queue.size() + "; " + length + " of type "
								+ type);
					}
					return sub;
				}
				if (Settings.traceMasterQueue) {
					Globals.log.reportProgress("No ready worker for task type "
							+ type);
				}
			}
			ix++;
		}
		return null;
	}

	synchronized boolean hasWork() {
		return !queue.isEmpty();
	}

	long[] getQueueIntervals() {
		final long res[] = new long[queueTypes.length];

		for (int ix = 0; ix < queueTypes.length; ix++) {
			res[ix] = queueTypes[ix].estimateQueueTime();
		}
		return res;
	}

	/**
	 * Clear the work queue.
	 * 
	 */
	synchronized void clear() {
		queue.clear();
	}

	/**
	 * Remove any copies of the given task instance from the master queue;
	 * somebody already completed it.
	 * 
	 * @param task
	 *            The task to remove.
	 */
	synchronized void removeDuplicates(TaskInstance task) {
		while (queue.remove(task)) {
			// Nothing.
		}
	}

	synchronized boolean hasRoom( int workers ) {
		return queue.size()<Settings.ROOM_PER_WORKER*workers;
	}
}
