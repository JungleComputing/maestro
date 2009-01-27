package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The list of workers of a master.
 * 
 * @author Kees van Reeuwijk
 */
final class NodeList {
    private final HashMap<IbisIdentifier, NodeInfo> ibisToNodeMap = new HashMap<IbisIdentifier, NodeInfo>();

    WorkerQueue workerQueue;

    NodeList(WorkerQueue taskInfoList) {
        this.workerQueue = taskInfoList;
    }

    /**
     * We know the given ibis has disappeared from the computation. Remove any
     * workers on that ibis.
     * 
     * @param theIbis
     *            The ibis that was gone.
     */
    ArrayList<TaskInstance> removeNode(IbisIdentifier theIbis) {
        if (Settings.traceWorkerList) {
            Globals.log.reportProgress("remove node " + theIbis);
        }
        ArrayList<TaskInstance> orphans = null;
        final NodeInfo node;
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
     *            The identifier o the ibis.
     * @param local
     *            Is this a local node?
     * @return The newly created Node info for this node.
     */
    synchronized NodeInfo registerNode(IbisIdentifier theIbis, boolean local) {
        NodeInfo info = ibisToNodeMap.get(theIbis);
        if (info != null) {
            return info;
        }
        info = new NodeInfo(theIbis, workerQueue, local);
        workerQueue.registerNode(info);
        ibisToNodeMap.put(theIbis, info);
        return info;
    }

    /**
     * Register a task result in the info of the worker that handled it.
     * 
     * @param result
     *            The task result.
     * @return The task instance that was completed if it may have duplicates,
     *         or <code>null</code>
     */
    TaskInstance registerTaskCompleted(TaskCompletedMessage result) {
        final NodeInfo node;
        synchronized (this) {
            node = ibisToNodeMap.get(result.source);
        }
        if (node == null) {
            Globals.log.reportError("Task completed message from unknown node "
                    + result.source);
            return null;
        }
        final TaskInstance task = node.registerTaskCompleted(result);
        node.registerAsCommunicating();
        return task;
    }

    /**
     * Register the fact that the worker has received a task.
     * 
     * @param msg
     *            The message.
     */
    void registerTaskReceived(TaskReceivedMessage msg) {
        final NodeInfo node;
        synchronized (this) {
            node = ibisToNodeMap.get(msg.source);
        }
        if (node == null) {
            Globals.log
                    .reportInternalError("Task received message from unknown node "
                            + msg.source);
            return;
        }
        node.registerTaskReceived(msg);
        node.registerAsCommunicating();
    }

    /**
     * Register that a tasked has failed.
     * 
     * @param ibis
     *            The ibis that failed to execute the task.
     * @param taskId
     *            The id of the failed task.
     * @return The task instance that was executed.
     */
    TaskInstance registerTaskFailed(IbisIdentifier ibis, long taskId) {
        final NodeInfo node;
        synchronized (this) {
            node = ibisToNodeMap.get(ibis);
        }
        if (node == null) {
            Globals.log.reportError("Task failed message from unknown node "
                    + ibis);
            return null;
        }
        node.registerAsCommunicating();
        return node.registerTaskFailed(taskId);
    }

    /**
     * Given a print stream, print some statistics about the workers to this
     * stream.
     * 
     * @param out
     *            The stream to print to.
     */
    void printStatistics(PrintStream out) {
        for (final Map.Entry<IbisIdentifier, NodeInfo> entry : ibisToNodeMap
                .entrySet()) {
            final NodeInfo wi = entry.getValue();
            if (wi != null) {
                wi.printStatistics(out);
            }
        }
    }

    protected void setSuspect(IbisIdentifier theIbis) {
        final NodeInfo wi = get(theIbis);

        if (wi != null) {
            wi.setSuspect();
        }
    }

    /**
     * Given an ibis, return its NodeInfo. If necessary create one.
     * 
     * @param source
     *            The ibis.
     * @return Its NodeInfo.
     */
    private NodeInfo getNodeInfo(IbisIdentifier source) {
        NodeInfo nodeInfo = ibisToNodeMap.get(source);
        if (nodeInfo == null) {
            nodeInfo = registerNode(source, false); // This must be a remote
            // node, since we certainly
            // have registered the local
            // node.
        }
        return nodeInfo;
    }

    /**
     * Given an ibis, return its NodeInfo. If necessary create one. The
     * operation is atomic wrt this node list.
     * 
     * @param source
     *            The ibis.
     * @return Its NodeInfo.
     */
    synchronized NodeInfo get(IbisIdentifier id) {
        return getNodeInfo(id);
    }

    boolean registerAsCommunicating(IbisIdentifier ibisIdentifier) {
        final NodeInfo nodeInfo = get(ibisIdentifier);
        return nodeInfo.registerAsCommunicating();
    }

    /**
     * Check the deadlines of the nodes.
     */
    synchronized void checkDeadlines(long now) {
        for (final Map.Entry<IbisIdentifier, NodeInfo> entry : ibisToNodeMap
                .entrySet()) {
            final NodeInfo nodeInfo = entry.getValue();

            if (nodeInfo != null) {
                nodeInfo.checkDeadlines(now);
            }
        }
    }

    /**
     * Returns a table of local information for every known node.
     * 
     * @return The information table.
     */
    synchronized HashMap<IbisIdentifier, LocalNodeInfo> getLocalNodeInfo() {
        final HashMap<IbisIdentifier, LocalNodeInfo> res = new HashMap<IbisIdentifier, LocalNodeInfo>();
        for (final Map.Entry<IbisIdentifier, NodeInfo> entry : ibisToNodeMap
                .entrySet()) {
            final NodeInfo nodeInfo = entry.getValue();

            res.put(entry.getKey(), nodeInfo.getLocalInfo());
        }
        return res;
    }

    synchronized int size() {
        return ibisToNodeMap.size();
    }
}
