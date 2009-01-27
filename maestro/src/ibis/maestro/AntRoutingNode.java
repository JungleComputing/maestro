/**
 * 
 */
package ibis.maestro;

import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Kees van Reeuwijk
 * 
 */
public class AntRoutingNode extends Node {
    private final Flag doUpdateRecentMasters = new Flag(false);

    private final AntRoutingTable antRoutingTable = new AntRoutingTable();

    /**
     * Constructs a new Maestro node using the given list of jobs. Optionally
     * try to get elected as maestro.
     * 
     * @param jobs
     *            The jobs that should be supported in this node.
     * @param runForMaestro
     *            If true, try to get elected as maestro.
     * @throws IbisCreationFailedException
     *             Thrown if for some reason we cannot create an ibis.
     * @throws IOException
     *             Thrown if for some reason we cannot communicate.
     */
    public AntRoutingNode(JobList jobs, boolean runForMaestro)
            throws IbisCreationFailedException, IOException {
        super(jobs, runForMaestro);
        recentMasterList.register(Globals.localIbis.identifier());
        super.startThreads();
        if (Settings.traceNodes) {
            Globals.log.log("Started a Maestro node using ant routing");
        }
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
    @Override
    public int waitForReadyNodes(int n, long maximalWaitTime) {
        return gossiper.waitForReadyNodes(n, maximalWaitTime);
    }

    @Override
    protected void failNode(RunTaskMessage message, Throwable t) {
        super.failNode(message, t);
        gossiper.failTask(message.taskInstance.type);
    }

    @Override
    protected RunTaskMessage getWork() {
        return workerQueue.remove(gossiper);
    }

    @Override
    protected void waitForWorkThreadsToTerminate() {
        super.waitForWorkThreadsToTerminate();
        gossiper.setStopped();
    }

    /**
     * Given an input and a list of possible jobs to execute, submit this input
     * as a job with the best promised completion time. If
     * <code>submitIfBusy</code> is set, also consider jobs where all workers
     * are currently busy.
     * 
     * @param input
     *            The input of the job.
     * @param submitIfBusy
     *            If set, also consider jobs for which all workers are currently
     *            busy.
     * @param listener
     *            The completion listener for this job.
     * @param choices
     *            The list of job choices.
     * @return <code>true</code> if the job could be submitted.
     */
    @Override
    public boolean submit(Object input, Serializable userId,
            boolean submitIfBusy, JobCompletionListener listener,
            Job... choices) {
        int choice;

        if (choices.length == 0) {
            // No choices? Obviously we won't be able to submit this one.
            return false;
        }
        if (choices.length == 1 && submitIfBusy) {
            choice = 0;
        } else {
            final TaskType types[] = new TaskType[choices.length];

            for (int ix = 0; ix < choices.length; ix++) {
                final Job job = choices[ix];

                types[ix] = job.getFirstTaskType();
            }
            choice = 0; // FIXME: do something smarter for task choice in ant
            // routing.
            if (choice < 0) {
                // Couldn't submit the job.
                return false;
            }
        }
        final Job job = choices[choice];
        job.submit(this, input, userId, listener, new ArrayList<AntPoint>());
        return true;
    }

    private final RecentMasterList recentMasterList = new RecentMasterList();

    private final Counter updateMessageCount = new Counter();

    /** On a locked queue, try to send out as many tasks as we can. */
    @Override
    protected void drainMasterQueue() {
        if (masterQueue.isEmpty()) {
            // Nothing to do, don't bother with the gossip.
            return;
        }
        while (true) {
            NodeInfo worker;
            long taskId;
            IbisIdentifier node;
            TaskInstance task;

            synchronized (antRoutingTable) {
                final HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap = nodes
                        .getLocalNodeInfo();
                final Submission submission = masterQueue.getAntSubmission(
                        localNodeInfoMap, antRoutingTable);
                if (submission == null) {
                    break;
                }
                node = submission.worker;
                task = submission.task;
                worker = nodes.get(node);
                taskId = nextTaskId++;

                worker.registerTaskStart(task, taskId,
                        submission.predictedDuration);
            }
            if (Settings.traceMasterQueue || Settings.traceSubmissions) {
                Globals.log.reportProgress("Submitting task " + task + " to "
                        + node);
            }
            final ArrayList<AntPoint> antTrail = task.antTrail;
            final AntPoint point = new AntPoint(Globals.localIbis.identifier(),
                    null, System.nanoTime(), task.type.index);
            antTrail.add(point);
            final RunTaskMessage msg = new RunTaskMessage(node, task, taskId,
                    antTrail);
            final boolean ok = sendPort.send(node, msg);
            if (ok) {
                submitMessageCount.add();
            } else {
                // Try to put the paste back in the tube.
                // The send port has already registered the trouble.
                masterQueue.add(msg.taskInstance);
                worker.retractTask(taskId);
            }
        }
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * 
     * @param m
     *            The update message.
     */
    @Override
    protected void handleNodeUpdateMessage(UpdateNodeMessage m) {
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Received node update message " + m);
        }
        gossiper.registerGossip(m.update, m.update.source);
        // FIXME: move to gossiper.
    }

    @Override
    protected void updateRecentMasters() {
        final NodePerformanceInfo update = gossiper.getLocalUpdate();
        final UpdateNodeMessage msg = new UpdateNodeMessage(update);
        for (final IbisIdentifier ibis : recentMasterList.getArray()) {
            if (Settings.traceUpdateMessages) {
                Globals.log.reportProgress("Sending " + msg + " to " + ibis);
            }
            sendPort.send(ibis, msg);
            updateMessageCount.add();
        }
    }

    /**
     * Registers the ibis with the given identifier as one that has left the
     * computation.
     * 
     * @param theIbis
     *            The ibis that has left.
     */
    @Override
    protected void registerIbisLeft(IbisIdentifier theIbis) {
        gossiper.removeNode(theIbis);
        antRoutingTable.removeNode(theIbis);
        recentMasterList.remove(theIbis);
        super.registerIbisLeft(theIbis);
    }

    /** Print some statistics about the entire worker run. */
    @Override
    synchronized void printStatistics(PrintStream s) {
        super.printStatistics(s);
        gossiper.printStatistics(s);
        s.printf("update        messages:   %5d sent\n", updateMessageCount
                .get());
    }

    /**
     * @param message
     *            The task that was run.
     * @param result
     *            The result of the task.
     * @param runMoment
     *            The moment the task was started.
     */
    @SuppressWarnings("unchecked")
    @Override
    void handleTaskResult(RunTaskMessage message, Object result, long runMoment) {
        final long taskCompletionMoment = System.nanoTime();

        final TaskType type = message.taskInstance.type;
        taskResultMessageCount.add();

        final TaskType nextTaskType = jobs.getNextTaskType(type);
        final ArrayList<AntPoint> oldAntTrail = message.taskInstance.antTrail;
        if (nextTaskType == null) {
            // This was the final step. Report back the result.
            final JobInstanceIdentifier identifier = message.taskInstance.jobInstance;
            boolean ok = sendJobResultMessage(identifier, result);
            if (!ok) {
                // Could not send the result message. We're in trouble.
                // Just try again.
                ok = sendJobResultMessage(identifier, result);
                if (!ok) {
                    // Nothing we can do, we give up.
                    Globals.log
                            .reportError("Could not send job result message to "
                                    + identifier);
                }
            }
            handleAntBackTrail(oldAntTrail, oldAntTrail.size() - 1);
        } else {
            // There is a next step to take.
            final ArrayList<AntPoint> antTrail = (ArrayList<AntPoint>) oldAntTrail
                    .clone();
            final TaskInstance nextTask = new TaskInstance(
                    message.taskInstance.jobInstance, nextTaskType, result,
                    antTrail);
            submit(nextTask);
        }

        // Update statistics.
        final long computeInterval = taskCompletionMoment - runMoment;
        final long averageComputeTime = workerQueue.countTask(type,
                computeInterval);
        gossiper.setComputeTime(type, averageComputeTime);
        runningTasks.down();
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final long queueInterval = runMoment - message.arrivalMoment;
            Globals.log.reportProgress("Completed " + message.taskInstance
                    + "; queueInterval="
                    + Utils.formatNanoseconds(queueInterval)
                    + "; runningTasks=" + runningTasks);
        }
        final long workerDwellTime = taskCompletionMoment
                - message.arrivalMoment;
        if (traceStats) {
            final double now = 1e-9 * (System.nanoTime() - startTime);
            System.out.println("TRACE:workerDwellTime " + type + " " + now
                    + " " + 1e-9 * workerDwellTime);
        }
        if (!deadNodes.contains(message.source)) {
            final Message msg = new TaskCompletedMessage(message.taskId,
                    workerDwellTime);
            boolean ok = sendPort.send(message.source, msg);

            if (!ok) {
                // Could not send the result message. We're desperate.
                // First simply try again.
                ok = sendPort.send(message.source, msg);
                if (!ok) {
                    // Unfortunately, that didn't work.
                    // TODO: think up another way to recover from failed result
                    // report.
                    Globals.log
                            .reportError("Failed to send task completed message to "
                                    + message.source);
                }
            }
        }
        doUpdateRecentMasters.set();
    }

    private void sendAntBackTrail(ArrayList<AntPoint> trail, int ix) {
        final Message msg = new AntInfoMessage(new ArrayList<AntPoint>(trail
                .subList(0, ix)));
        final IbisIdentifier dest = trail.get(ix).masterIbis;
        super.outgoingMessageQueue.add(dest, msg);
    }

    private void handleAntBackTrail(ArrayList<AntPoint> trail, int ix) {
        while (true) {
            if (ix < 0) {
                return;
            }
            final AntPoint p = trail.get(ix);
            if (!p.masterIbis.equals(Globals.localIbis.identifier())) {
                // Not for us; send it to the node that handled it.
                break;
            }
            antRoutingTable.handleAntPoint(p);
            ix--; // See if we can handle the previous one in the trail as
                    // well.
        }
        sendAntBackTrail(trail, ix);
    }

    /**
     * Handle a message containing a new task to run.
     * 
     * @param msg
     *            The message to handle.
     */
    @Override
    protected void handleRunTaskMessage(RunTaskMessage msg) {
        final IbisIdentifier source = msg.source;
        final boolean isDead = nodes.registerAsCommunicating(source);
        if (!isDead && !source.equals(Globals.localIbis.identifier())) {
            recentMasterList.register(source);
        }
        doUpdateRecentMasters.set();
        postTaskReceivedMessage(source, msg.taskId);
        final int length = workerQueue.add(msg);
        if (gossiper != null) {
            gossiper.setQueueLength(msg.taskInstance.type, length);
        }
    }

    /**
     * Do all updates of the node adminstration that we can.
     * 
     */
    @Override
    protected void updateAdministration() {
        if (doUpdateRecentMasters.getAndReset()) {
            updateRecentMasters();
        }
        super.updateAdministration();
    }

    /**
     * @param theIbis
     *            The ibis that has joined.
     */
    @Override
    protected void registerIbisJoined(IbisIdentifier theIbis) {
        super.registerIbisJoined(theIbis);
        final NodeInfo info = nodes.get(theIbis);
        antRoutingTable.addNode(info);
    }

    @Override
    protected void registerNewGossipHasArrived() {
        // We're not interested.
    }

    /**
     * A worker has sent use a completion message for a task. Process it.
     * 
     * @param result
     *            The message.
     */
    @Override
    protected void handleTaskCompletedMessage(TaskCompletedMessage result) {
        super.handleTaskCompletedMessage(result);
        doUpdateRecentMasters.set();
    }

    @Override
    void handleAntInfoMessage(AntInfoMessage antInfoMessage) {
        final int sz = antInfoMessage.antPoints.size() - 1;
        handleAntBackTrail(antInfoMessage.antPoints, sz);
    }
}
