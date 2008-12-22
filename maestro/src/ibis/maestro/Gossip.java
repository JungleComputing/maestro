package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Gossip information.
 * 
 * @author Kees van Reeuwijk.
 */
class Gossip {
	private final ArrayList<NodePerformanceInfo> gossipList = new ArrayList<NodePerformanceInfo>();
	private final NodePerformanceInfo localPerformanceInfo;

	GossipMessage constructMessage(IbisIdentifier target, boolean needsReply) {
		final NodePerformanceInfo content[] = getCopy();
		return new GossipMessage(target, content, needsReply);
	}

	Gossip(JobList jobs) {
		final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
		final int sz = Globals.allTaskTypes.length;
		final long completionInfo[] = new long[sz];
		final WorkerQueueInfo queueInfo[] = new WorkerQueueInfo[sz];
		final long taskTimes[] = jobs.getInitialTaskTimes();
		for (int i = 0; i < sz; i++) {
			queueInfo[i] = new WorkerQueueInfo(0, 0, 0L, taskTimes[i]);
		}
		localPerformanceInfo = new NodePerformanceInfo(completionInfo,
				queueInfo, Globals.localIbis.identifier(), numberOfProcessors,
				System.nanoTime());
		gossipList.add(localPerformanceInfo);
		final int indexLists[][] = jobs.getIndexLists();

		for (final int indexList[] : indexLists) {
			int t = 0;

			for (final int typeIndex : indexList) {
				localPerformanceInfo.completionInfo[typeIndex] = t;
				t += taskTimes[typeIndex];
			}
		}
		localPerformanceInfo.timeStamp = System.nanoTime();
	}

	synchronized boolean isEmpty() {
		return gossipList.isEmpty();
	}

	synchronized NodePerformanceInfo[] getCopy() {
		final NodePerformanceInfo content[] = new NodePerformanceInfo[gossipList
		                                                              .size()];
		for (int i = 0; i < content.length; i++) {
			content[i] = gossipList.get(i).getDeepCopy();
		}
		return content;
	}

	private int searchInfo(IbisIdentifier ibis) {
		for (int ix = 0; ix < gossipList.size(); ix++) {
			final NodePerformanceInfo i = gossipList.get(ix);

			if (i.source.equals(ibis)) {
				return ix;
			}
		}
		return -1;
	}

	/**
	 * Returns the best average completion time for this task after it has been
	 * sent by the master. We compute this by taking the minimum over all our
	 * workers.
	 * 
	 * @param ix
	 *            The index of the type we're computing the completion time for.
	 * @param nextIx
	 *            The index of the type after the current one, or
	 *            <code>-1</code> if there isn't one.
	 * @param localNodeInfoMap
	 *            A table with locally collected performance info for all worker
	 *            nodes.
	 * @return The best average completion time of our workers.
	 */
	synchronized long getBestCompletionTimeAfterMasterQueue(int ix, int nextIx,
			HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap) {
		long res = Long.MAX_VALUE;

		for (final NodePerformanceInfo node : gossipList) {
			final LocalNodeInfo info = localNodeInfoMap.get(node.source);

			if (info != null) {
				final long xmitTime = info.getTransmissionTime(ix);
				final long val = Utils.safeAdd(xmitTime, node.getCompletionOnWorker(
						ix, nextIx));

				if (val < res) {
					res = val;
				}
			}
		}
		return res;
	}

	/**
	 * Given the current queue intervals on the master, recompute in-place the
	 * completion intervals for the various task types. The completion interval
	 * is defined as the time it will take a task on a given master from the
	 * moment it enters its master queue to the moment its entire job is
	 * completed.
	 * 
	 * @param masterQueueIntervals
	 *            The time in nanoseconds for each task it is estimated to dwell
	 *            in the master queue.
	 * @param localNodeInfoMap
	 */
	synchronized void recomputeCompletionTimes(long masterQueueIntervals[],
			JobList jobs,
			HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap) {
		final int indexLists[][] = jobs.getIndexLists();

		for (final int indexList[] : indexLists) {
			int nextIndex = -1;

			for (final int typeIndex : indexList) {
				final long masterQueueInterval = masterQueueIntervals == null ? 0L : masterQueueIntervals[typeIndex];
				final long t = Utils.safeAdd(masterQueueInterval,
						getBestCompletionTimeAfterMasterQueue(typeIndex,
								nextIndex, localNodeInfoMap));
				localPerformanceInfo.completionInfo[typeIndex] = t;
				nextIndex = typeIndex;
			}
		}
		localPerformanceInfo.timeStamp = System.nanoTime();
	}

	/**
	 * Registers the given information in our collection of gossip.
	 * 
	 * @param update
	 *            The information to register.
	 * @return True iff we learned something new.
	 */
	synchronized boolean register(NodePerformanceInfo update) {
		final int ix = searchInfo(update.source);
		if (ix >= 0) {
			// This is an update for the same node.
			final NodePerformanceInfo i = gossipList.get(ix);

			if (update.timeStamp > i.timeStamp) {
				// This is more recent info, overwrite the old entry.
				if (Settings.traceGossip) {
					Globals.log.reportProgress("Updated gossip info about "
							+ update.source + ": " + update.toString());
				}
				gossipList.set(ix, update);
				return true;
			}
			return false;
		}
		if (Settings.traceGossip) {
			Globals.log.reportProgress("Got info about new node "
					+ update.source);
		}
		// If we reach this point, we didn't have info about this node.
		gossipList.add(update);
		this.notifyAll(); // Wake any waiters for ready nodes
		return true;
	}

	synchronized void removeInfoForNode(IbisIdentifier ibis) {
		if (false) {
			// TODO: enable again or remove method.
			final int ix = searchInfo(ibis);

			if (ix >= 0) {
				gossipList.remove(ix);
			}
		}
	}

	/**
	 * Overwrite the worker queue info of our local information with the new
	 * info.
	 * 
	 * @param update
	 *            The new information.
	 */
	synchronized void registerWorkerQueueInfo(WorkerQueueInfo[] update) {
		localPerformanceInfo.workerQueueInfo = update;
		localPerformanceInfo.timeStamp = System.nanoTime();
	}

	synchronized NodePerformanceInfo getLocalUpdate() {
		return localPerformanceInfo.getDeepCopy();
	}

	synchronized void print(PrintStream s) {
		NodePerformanceInfo.printTopLabel(s);
		for (final NodePerformanceInfo entry : gossipList) {
			entry.print(s);
		}
	}

	synchronized int size() {
		return gossipList.size();
	}

	/**
	 * Given a number of nodes to wait for, keep waiting until we have gossip
	 * information about at least this many nodes, or until the given time has
	 * elapsed.
	 * 
	 * @param nodes
	 *            The number of nodes to wait for.
	 * @param maximalWaitTime
	 *            The maximal time in ms to wait for these nodes.
	 * @return The actual number of nodes there was information for at the
	 *         moment we stopped waiting.
	 */
	int waitForReadyNodes(int nodes, long maximalWaitTime) {
		final long deadline = System.currentTimeMillis() + maximalWaitTime;
		while (true) {
			final long now = System.currentTimeMillis();
			final long sleepTime = Math.max(1L, deadline - now);
			synchronized (this) {
				final int sz = size();
				Globals.log.reportProgress("There are now " + sz
						+ " ready workers");
				if (sz >= nodes || now > deadline) {
					return sz;
				}
				try {
					wait(sleepTime);
				} catch (final InterruptedException e) {
					// Ignore
				}
			}
		}
	}

	/**
	 * Given a task type, return the estimated completion time of this task.
	 * 
	 * @param type
	 *            The task type for which we want to know the completion time.
	 * @param submitIfBusy
	 *            If set, take into consideration processors that are currently
	 *            fully occupied with tasks.
	 * @param localNodeInfoMap
	 *            Local knowledge about the different nodes.
	 * @return The estimated completion time for the best worker.
	 */
	long computeCompletionTime(TaskType type, boolean submitIfBusy,
			HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap) {
		long bestTime = Long.MAX_VALUE;

		for (final NodePerformanceInfo info : gossipList) {
			final LocalNodeInfo localNodeInfo = localNodeInfoMap.get(info.source);

			if (localNodeInfo != null) {
				final long t = info.estimateJobCompletion(localNodeInfo, type,
						!submitIfBusy);
				if (t < bestTime) {
					bestTime = t;
				}
			}
		}
		return bestTime;
	}

	long getLocalTimestamp() {
		return localPerformanceInfo.timeStamp;
	}

	void localNodeFailTask(TaskType type) {
		localPerformanceInfo.failTask(type);
	}

	void setLocalComputeTime(TaskType type, long t) {
		localPerformanceInfo.setComputeTime(type, t);
	}

	void setQueueTimePerTask(TaskType type, long queueTimePerTask,
			int queueLength) {
		localPerformanceInfo.setQueueTimePerTask(type, queueTimePerTask,
				queueLength);
	}

	void setQueueLength(TaskType type, int queueLength) {
		localPerformanceInfo.setQueueLength(type, queueLength);
	}

}
