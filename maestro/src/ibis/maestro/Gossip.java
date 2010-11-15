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

    Gossip(JobList jobs, IbisIdentifier localIbis) {
        final int numberOfProcessors = Runtime.getRuntime()
                .availableProcessors();
        final int sz = jobs.getTypeCount();
        final TimeEstimate completionInfo[][] = buildCompletionInfo(jobs);
        final WorkerQueueInfo queueInfo[] = new WorkerQueueInfo[sz];
        final TimeEstimate jobTimes[] = jobs.getInitialJobTimes();
        for (int i = 0; i < sz; i++) {
            queueInfo[i] = new WorkerQueueInfo(0, 0, TimeEstimate.ZERO,
                    jobTimes[i]);
        }
        localPerformanceInfo = new NodePerformanceInfo(completionInfo,
                queueInfo, localIbis, numberOfProcessors, System.nanoTime());
        gossipList.add(localPerformanceInfo);
        localPerformanceInfo.timeStamp = System.nanoTime();
    }

    private TimeEstimate[][] buildCompletionInfo(JobList jobs) {
        final JobType types[] = jobs.getAllTypes();
        final TimeEstimate res[][] = new TimeEstimate[types.length][];

        for (int i = 0; i < types.length; i++) {
            final JobType t = types[i];
            final JobType todoList[] = jobs.getTodoList(t);
            final TimeEstimate l1[] = new TimeEstimate[todoList.length];

            res[i] = l1;
        }
        return res;
    }

    /**
     * Given the current queue intervals on the master, recompute in-place the
     * completion intervals for the various job types. The completion interval
     * is defined as the time it will take a job on a given master from the
     * moment it enters its master queue to the moment its entire job is
     * completed.
     * 
     * @param masterQueueIntervals
     *            The time in seconds for each job it is estimated to dwell in
     *            the master queue.
     * @param jobs
     *            The job types we know about.
     * @param localNodeInfoMap
     *            The local knowledge about the nodes in the system.
     */
    synchronized void recomputeCompletionTimes(
            TimeEstimate masterQueueIntervals[], JobList jobs,
            HashMap<IbisIdentifier, LocalNodeInfoList> localNodeInfoMap) {
        final JobType types[] = jobs.getAllTypes();

        for (int tix = 0; tix < types.length; tix++) {
            final JobType todoList[] = jobs.getTodoList(types[tix]);

            int ix = todoList.length;
            int nextIndex = -1;

            while (ix > 0) {
                ix--;

                final TimeEstimate masterQueueInterval = masterQueueIntervals == null ? TimeEstimate.ZERO
                        : masterQueueIntervals[ix];
                final TimeEstimate bestCompletionTimeAfterMasterQueue = getBestCompletionTimeAfterMasterQueue(
                        tix, ix, nextIndex, localNodeInfoMap);
                final TimeEstimate t = masterQueueInterval
                        .addIndependent(bestCompletionTimeAfterMasterQueue);
                localPerformanceInfo.completionInfo[tix][ix] = t;
                nextIndex = ix;
            }
        }
        localPerformanceInfo.timeStamp = System.nanoTime();
    }

    synchronized boolean isEmpty() {
        return gossipList.isEmpty();
    }

    synchronized NodePerformanceInfo[] getCopy() {
        final int size = gossipList.size();
        final NodePerformanceInfo content[] = new NodePerformanceInfo[size];
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
     * Returns the best average completion time for this job after it has been
     * sent by the master. We compute this by taking the minimum over all our
     * workers.
     * 
     * @param todoIx
     *            The index in the todo list.
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
    private TimeEstimate getBestCompletionTimeAfterMasterQueue(int todoIx,
            int ix, int nextIx,
            HashMap<IbisIdentifier, LocalNodeInfoList> localNodeInfoMap) {
        TimeEstimate res = null;

        for (final NodePerformanceInfo node : gossipList) {
            final LocalNodeInfoList info = localNodeInfoMap.get(node.source);

            if (info != null) {
                final TimeEstimate xmitTime = info.getTransmissionTime(ix);
                final TimeEstimate val = xmitTime.addIndependent(node
                        .getCompletionOnWorker(todoIx, ix, nextIx));

                if (val < res) {
                    res = val;
                }
            }
        }
        return res;
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

    synchronized void removeNode(IbisIdentifier ibis) {
        final int ix = searchInfo(ibis);

        if (ix >= 0) {
            gossipList.remove(ix);
        }
    }

    synchronized NodePerformanceInfo getLocalUpdate() {
        return localPerformanceInfo.getDeepCopy();
    }

    synchronized void print(PrintStream s, JobList jobs) {
        NodePerformanceInfo.printTopLabel(s, jobs);
        for (final NodePerformanceInfo entry : gossipList) {
            entry.print(s);
        }
    }

    private synchronized int size() {
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

    long getLocalTimestamp() {
        return localPerformanceInfo.timeStamp;
    }

    void localNodeFailJob(JobType type) {
        localPerformanceInfo.failJob(type);
    }

    void setLocalComputeTime(JobType type, TimeEstimate t) {
        localPerformanceInfo.setComputeTime(type, t);
    }

    void setWorkerQueueTimePerJob(JobType type, TimeEstimate queueTimePerJob,
            int queueLength) {
        localPerformanceInfo.setWorkerQueueTimePerJob(type, queueTimePerJob,
                queueLength);
    }

    boolean setWorkerQueueLength(JobType type, int queueLength) {
        return localPerformanceInfo.setWorkerQueueLength(type, queueLength);
    }

}
