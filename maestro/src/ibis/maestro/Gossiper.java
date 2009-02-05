package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * A thread that gossips with the other ibises to exchange performance
 * information. The strategy is to try and keep recent information on all nodes
 * by exchanging information with all other nodes.
 * 
 * @author Kees van Reeuwijk.
 */
class Gossiper extends Thread {
    private boolean stopped = false;

    private final GossipNodeList nodes = new GossipNodeList();

    private final IbisSet deadNodes = new IbisSet();

    private final Gossip gossip;

    private final UpDownCounter gossipQuotum;

    private final Counter messageCount = new Counter();

    private final Counter gossipItemCount = new Counter();

    private final Counter newGossipItemCount = new Counter();

    private final Counter failedGossipMessageCount = new Counter();

    private final LinkedList<IbisIdentifier> nodesToReplyTo = new LinkedList<IbisIdentifier>();

    private final PacketSendPort sendPort;

    private double adminTime = 0;

    private double sendTime = 0;

    private long sentBytes = 0;

    Gossiper(PacketSendPort sendPort, boolean isMaestro, JobList jobs) {
        super("Maestro gossiper thread");
        this.sendPort = sendPort;
        this.gossipQuotum = new UpDownCounter(isMaestro ? 40 : 4);
        setDaemon(true);
        setPriority(Thread.MAX_PRIORITY);
        gossip = new Gossip(jobs);
    }

    NodePerformanceInfo[] getGossipCopy() {
        return gossip.getCopy();
    }

    private void sendGossip(IbisIdentifier target, boolean needsReply) {
        final GossipMessage msg = gossip.constructMessage(target, needsReply);
        final double startTime = Utils.getPreciseTime();
        msg.sendMoment = startTime;
        if (Settings.traceGossip) {
            Globals.log.reportProgress("Gossiper: sending message to " + target
                    + "; needsReply=" + needsReply);
            if (target.equals(Globals.localIbis.identifier())) {
                Globals.log.reportInternalError("Sending gossip to ourselves");
            }
        }
        if (!sendPort.sendNonessentialMessage(target, msg)) {
            retrySendGossipMessage(msg, startTime);
        }
        gossipQuotum.down();
        messageCount.add();
    }

    /**
     * @param msg
     *            The gossip message to send.
     * @param startTime
     *            The supposed start time of the transmission.
     */
    private void retrySendGossipMessage(GossipMessage msg, double startTime) {
        SendPort port = null;
        try {
            long len;
            final IbisIdentifier theIbis = msg.destination;
            port = Globals.localIbis.createSendPort(PacketSendPort.portType);
            port.connect(theIbis, Globals.receivePortName,
                    Settings.OPTIONAL_COMMUNICATION_TIMEOUT, false);
            final double setupTime = Utils.getPreciseTime();
            final WriteMessage writeMessage = port.newMessage();
            try {
                writeMessage.writeObject(msg);
            } finally {
                len = writeMessage.finish();
            }
            final double stopTime = Utils.getPreciseTime();
            if (Settings.traceSends || Settings.traceGossip) {
                Globals.log.reportProgress("Gossiper: sent message of " + len
                        + " bytes in "
                        + Utils.formatSeconds(stopTime - setupTime)
                        + "; setup time "
                        + Utils.formatSeconds(setupTime - startTime) + ": "
                        + msg);
            }
            synchronized (this) {
                adminTime += (setupTime - startTime);
                sendTime += (stopTime - setupTime);
                if (len > 0) {
                    sentBytes += len;
                }
            }
        } catch (final IOException e) {
            failedGossipMessageCount.add();
        } finally {
            try {
                if (port != null) {
                    port.close();
                }
            } catch (final Throwable x) {
                // Nothing we can do.
            }
        }
    }

    private void sendCurrentGossipMessages() {

        if (gossip.isEmpty()) {
            // There is nothing in the gossip yet, don't waste
            // our quotum.
            return;
        }
        while (gossipQuotum.isAbove(0)) {
            final long now = System.currentTimeMillis();
            final IbisIdentifier target = nodes.getStaleNode(now);
            if (target == null) {
                // Nobody to gossip. Stop.
                break;
            }
            if (isStopped()) {
                return;
            }
            sendGossip(target, true);
        }
    }

    private long computeWaitTimeInMilliseconds() {
        if (gossipQuotum.isAbove(0)) {
            return nodes.computeWaitTimeInMilliseconds();
        }
        return 0;
    }

    /** Runs this thread. */
    @Override
    public void run() {
        while (!isStopped()) {
            drainReplyQueue();
            sendCurrentGossipMessages();
            final long waittime = computeWaitTimeInMilliseconds();
            // We are not supposed to wait if there are
            // still messages in the current list, but a perverse
            // scheduler might have allowed a thread to add to the
            // queue after our last check.
            // We don't have to check the future queue, since only
            // sendAMessage can add to it, and if we miss a deadline
            // of a waiting message we won't do much damage.
            try {
                if (Settings.traceGossip) {
                    if (waittime == 0) {
                        Globals.log
                                .reportProgress("Gossiper: waiting indefinitely");
                    } else {
                        Globals.log.reportProgress("Gossiper: waiting "
                                + waittime + " ms");
                    }
                }
                synchronized (this) {
                    wait(waittime);
                }
            } catch (final InterruptedException e) {
                // ignore.
            }
        }
        if (Settings.traceGossip) {
            Globals.log
                    .reportProgress("Gossiper: has stopped, thread ends now");
        }
    }

    void registerNode(IbisIdentifier ibis) {
        if (ibis.equals(Globals.localIbis.identifier())) {
            // I'm not going to gossip to myself.
            return;
        }
        nodes.add(ibis);
        gossipQuotum.up(2);
        synchronized (this) {
            this.notifyAll();
        }
    }

    void removeNode(IbisIdentifier ibis) {
        deadNodes.add(ibis);
        nodes.remove(ibis);
        gossip.removeNode(ibis);
        synchronized (this) {
            this.notifyAll();
        }
    }

    boolean registerGossip(NodePerformanceInfo update, IbisIdentifier source) {
        if (deadNodes.contains(update.source)) {
            // Somebody sent us gossip about a dead node.Ignore it.
            return false;
        }
        gossipItemCount.add();
        final boolean isnew = gossip.register(update);
        if (isnew) {
            gossipQuotum.up(2);
            if (update.source.equals(Globals.localIbis.identifier())) {
                // Somebody sent us info about ourselves, if it's too old,
                // send that node an update,
                final long staleness = gossip.getLocalTimestamp()
                        - update.timeStamp;
                if (staleness > Settings.GOSSIP_EXPIRATION_IN_CLUSTER
                        && source != null) {
                    nodes.needsUrgentUpdate(source);
                    gossipQuotum.up();
                }
            } else {
                newGossipItemCount.add();
                nodes.hadRecentUpdate(update.source);
            }
            synchronized (this) {
                this.notifyAll();
            }
        }
        return isnew;
    }

    void recomputeCompletionTimes(double masterQueueIntervals[], JobList jobs,
            HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap) {
        gossip.recomputeCompletionTimes(masterQueueIntervals, jobs,
                localNodeInfoMap);
    }

    void addQuotum() {
        gossipQuotum.up();
        synchronized (this) {
            notifyAll();
        }
    }

    synchronized void printStatistics(PrintStream s) {
        s.println("Sent " + messageCount.get() + " gossip messages, with "
                + failedGossipMessageCount.get() + " failures, received "
                + gossipItemCount.get() + " gossip items, "
                + newGossipItemCount.get() + " new");
        s.println("Sent " + Utils.formatByteCount(sentBytes) + " in "
                + Utils.formatSeconds(sendTime) + ", administration time "
                + Utils.formatSeconds(adminTime));
        gossip.print(s);
    }

    /**
     * Directly sends a gossip message to the given node.
     * 
     * @param source
     *            The node to send the gossip to.
     */
    private void queueGossipReply(IbisIdentifier source) {
        if (Settings.traceGossip) {
            Globals.log.reportProgress("Gossiper: send a reply to " + source);
        }
        synchronized (nodesToReplyTo) {
            nodesToReplyTo.add(source);
        }
        synchronized (this) {
            this.notifyAll();
        }
    }

    private void drainReplyQueue() {
        while (true) {
            IbisIdentifier target;

            synchronized (nodesToReplyTo) {
                if (nodesToReplyTo.isEmpty()) {
                    return;
                }
                target = nodesToReplyTo.remove();
            }
            sendGossip(target, false);
        }
    }

    void registerWorkerQueueInfo(WorkerQueueInfo[] update) {
        gossip.registerWorkerQueueInfo(update);
    }

    /**
     * Returns performance info about the local node. Used for rapid
     * updates for nodes we're directly communicating with.
     * @return The local node performance info.
     */
    NodePerformanceInfo getLocalUpdate() {
        return gossip.getLocalUpdate();
    }

    /**
     * Given a number of nodes to wait for, keep waiting until we have gossip
     * information about at least this many nodes, or until the given time has
     * elapsed.
     * 
     * @param n
     *            The number of nodes to wait for.
     * @param maximalWaitTime
     *            The maximal time in ms to wait for these nodes.
     * @return The actual number of nodes there was information for at the
     *         moment we stopped waiting.
     */
    int waitForReadyNodes(int n, long maximalWaitTime) {
        return gossip.waitForReadyNodes(n, maximalWaitTime);
    }

    synchronized void setStopped() {
        stopped = true;
        notifyAll();
    }

    private synchronized boolean isStopped() {
        return stopped;
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
     * @return
     */
    private double computeCompletionTime(TaskType type, boolean submitIfBusy,
            HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap) {
        return gossip.computeCompletionTime(type, submitIfBusy,
                localNodeInfoMap);
    }

    int selectFastestTask(TaskType[] types, boolean submitIfBusy,
            HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap) {
        int bestIx = -1;
        double bestTime = Double.POSITIVE_INFINITY;
        for (int ix = 0; ix < types.length; ix++) {
            final TaskType type = types[ix];
            final double t = computeCompletionTime(type, submitIfBusy,
                    localNodeInfoMap);
            if (t < bestTime) {
                bestTime = t;
                bestIx = ix;
            }
        }
        return bestIx;
    }

    void failTask(TaskType type) {
        gossip.localNodeFailTask(type);
        addQuotum();
    }

    void setComputeTime(TaskType type, double t) {
        gossip.setLocalComputeTime(type, t);
        addQuotum();
    }

    void setWorkerQueueTimePerTask(TaskType type, double queueTimePerTask,
            int queueLength) {
        gossip.setWorkerQueueTimePerTask(type, queueTimePerTask, queueLength);
        addQuotum();
    }

    void setWorkerQueueLength(TaskType type, int queueLength) {
        gossip.setWorkerQueueLength(type, queueLength);
        addQuotum();
    }

    /**
     * Register the contents of the given gossip message.
     * @param m The gossip message to register.
     * @return True iff this message change the gossip info.
     */
    boolean registerGossipMessage(GossipMessage m) {
        boolean changed = false;
    
        if (Settings.traceNodeProgress || Settings.traceRegistration
                || Settings.traceGossip) {
            Globals.log.reportProgress("Received gossip message from "
                    + m.source + " with " + m.gossip.length + " items");
        }
        for (final NodePerformanceInfo i : m.gossip) {
            changed |= registerGossip(i, m.source);
        }
        if (m.needsReply) {
            if (!m.source.equals(Globals.localIbis.identifier())) {
                queueGossipReply(m.source);
            }
        }
        return changed;
    }

}
