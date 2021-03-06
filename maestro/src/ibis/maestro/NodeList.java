package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * The list of workers of a master.
 * 
 * @author Kees van Reeuwijk
 */
final class NodeList {
    private final HashMap<IbisIdentifier, WorkerInfo> ibisToNodeMap = new HashMap<IbisIdentifier, WorkerInfo>();

    private final WorkerQueue workerQueue;

    NodeList(final WorkerQueue jobInfoList) {
        workerQueue = jobInfoList;
    }

    /**
     * We know the given ibis has disappeared from the computation. Remove any
     * workers on that ibis.
     * 
     * @param theIbis
     *            The ibis that was gone.
     */
    ArrayList<JobInstance> removeNode(final IbisIdentifier theIbis) {
        if (Settings.traceWorkerList) {
            Globals.log.reportProgress("remove node " + theIbis);
        }
        ArrayList<JobInstance> orphans = null;
        final WorkerInfo node;
        synchronized (this) {
            node = ibisToNodeMap.get(theIbis);
        }

        if (node != null) {
            orphans = node.setDead();
        }
        return orphans;
    }

    /**
     * Add a new node to the list with the given ibis identifier.
     * 
     * @param theIbis
     *            The identifier of the ibis.
     * @param local
     *            Is this a local node?
     * @param jobTypeCount
     *            The total number of known job types.
     * @return The newly created Node info for this node.
     */
    synchronized WorkerInfo registerNode(final IbisIdentifier theIbis,
            final boolean local, final int jobTypeCount) {
        WorkerInfo info = ibisToNodeMap.get(theIbis);
        if (info != null) {
            return info;
        }
        info = new WorkerInfo(theIbis, workerQueue, local, jobTypeCount);
        workerQueue.registerNode(info);
        ibisToNodeMap.put(theIbis, info);
        return info;
    }

    /**
     * Register a job result in the info of the worker that handled it.
     * 
     * @param result
     *            The job result.
     * @return The job instance that was completed if it may have duplicates, or
     *         <code>null</code>
     */
    JobInstance registerJobCompleted(final JobList jobs,
            final JobCompletedMessage result) {
        final WorkerInfo node;
        synchronized (this) {
            node = ibisToNodeMap.get(result.source);
        }
        if (node == null) {
            Globals.log.reportError("Job completed message from unknown node "
                    + result.source);
            return null;
        }
        node.registerAsCommunicating();
        final JobInstance job = node.registerJobCompleted(jobs, result);
        return job;
    }

    /**
     * Register the fact that the worker has received a job.
     * 
     * @param msg
     *            The message.
     */
    void registerJobReceived(final JobReceivedMessage msg) {
        final WorkerInfo node;
        synchronized (this) {
            node = ibisToNodeMap.get(msg.source);
        }
        if (node == null) {
            Globals.log
                    .reportInternalError("Job received message from unknown node "
                            + msg.source);
            return;
        }
        node.registerJobReceived(msg);
        node.registerAsCommunicating();
    }

    /**
     * Register that a jobed has failed.
     * 
     * @param ibis
     *            The ibis that failed to execute the job.
     * @param jobId
     *            The id of the failed job.
     * @return The job instance that was executed.
     */
    JobInstance registerJobFailed(final IbisIdentifier ibis, final long jobId) {
        final WorkerInfo node;
        synchronized (this) {
            node = ibisToNodeMap.get(ibis);
        }
        if (node == null) {
            Globals.log.reportError("Job failed message from unknown node "
                    + ibis);
            return null;
        }
        node.registerAsCommunicating();
        return node.registerJobFailed(jobId);
    }

    /**
     * Given a print stream, print some statistics about the workers to this
     * stream.
     * 
     * @param out
     *            The stream to print to.
     */
    void printStatistics(final PrintStream out) {
        for (final Map.Entry<IbisIdentifier, WorkerInfo> entry : ibisToNodeMap
                .entrySet()) {
            final WorkerInfo wi = entry.getValue();
            if (wi != null) {
                wi.printStatistics(out);
            }
        }
    }

    protected void setSuspect(final IbisIdentifier theIbis) {
        final WorkerInfo wi = get(theIbis);

        if (wi != null) {
            wi.setSuspect();
        }
    }

    /**
     * Given an ibis, return its NodeInfo. If necessary create one. The
     * operation is atomic wrt this node list.
     * 
     * @param source
     *            The ibis.
     * @return Its NodeInfo.
     */
    synchronized WorkerInfo get(final IbisIdentifier id) {
        final WorkerInfo workerInfo = ibisToNodeMap.get(id);
        if (workerInfo == null) {
            final Object[] l = ibisToNodeMap.keySet().toArray();
            Globals.log.reportInternalError("Unknown node " + id
                    + "; known nodes are: " + Arrays.deepToString(l));
        }
        return workerInfo;
    }

    boolean registerAsCommunicating(final IbisIdentifier ibisIdentifier) {
        final WorkerInfo nodeInfo = get(ibisIdentifier);
        return nodeInfo.registerAsCommunicating();
    }

    /**
     * Returns a table of local information for every known node.
     * 
     * @return The information table.
     */
    synchronized HashMap<IbisIdentifier, LocalNodeInfoList> getLocalNodeInfo() {
        final HashMap<IbisIdentifier, LocalNodeInfoList> res = new HashMap<IbisIdentifier, LocalNodeInfoList>();
        for (final Map.Entry<IbisIdentifier, WorkerInfo> entry : ibisToNodeMap
                .entrySet()) {
            final WorkerInfo nodeInfo = entry.getValue();

            res.put(entry.getKey(), nodeInfo.getLocalInfo());
        }
        return res;
    }
}
