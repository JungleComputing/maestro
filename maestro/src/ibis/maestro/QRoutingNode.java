/**
 * 
 */
package ibis.maestro;

import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.HashMap;

/**
 * A node using QRouting.
 * 
 * @author Kees van Reeuwijk.
 */
public class QRoutingNode extends Node {
    private final Flag doUpdateRecentMasters = new Flag(false);

    private final Flag recomputeCompletionTimes = new Flag(false);

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
    QRoutingNode(JobList jobs, boolean runForMaestro)
            throws IbisCreationFailedException, IOException {
        super(jobs, runForMaestro);
        recentMasterList.register(Globals.localIbis.identifier());
        super.startThreads();
        if (Settings.traceNodes) {
            Globals.log.log("Started a Maestro node using Q routing");
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
            JobSequence... choices) {
        int choice;

        if (choices.length == 0) {
            // No choices? Obviously we won't be able to submit this one.
            return false;
        }
        if (choices.length == 1 && submitIfBusy) {
            waitForRoom();
            choice = 0;
        } else {
            final JobType types[] = new JobType[choices.length];

            waitForRoom();
            for (int ix = 0; ix < choices.length; ix++) {
                final JobSequence job = choices[ix];

                types[ix] = job.getFirstTaskType();
            }
            final HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap = nodes
                    .getLocalNodeInfo();
            choice = gossiper.selectFastestTask(types, submitIfBusy,
                    localNodeInfoMap);
            if (choice < 0) {
                // Couldn't submit the job.
                return false;
            }
        }
        final JobSequence job = choices[choice];
        job.submit(this, input, userId, listener, null);
        return true;
    }

    /**
     * This object only exists to lock the critical section in drainMasterQueue,
     * and prevent that two threads select the same next task to submit to a
     * worker.
     */
    private final Flag drainLock = new Flag(false);

    private final RecentMasterList recentMasterList = new RecentMasterList();

    private final Counter updateMessageCount = new Counter();

    /** On a locked queue, try to send out as many tasks as we can. */
    @Override
    protected void drainMasterQueue() {
        boolean changed = false;

        if (masterQueue.isEmpty()) {
            // Nothing to do, don't bother with the gossip.
            return;
        }
        while (true) {
            NodeInfo worker;
            long taskId;
            IbisIdentifier node;
            JobInstance task;

            synchronized (drainLock) {
                // The entire operation
                // - get a submission
                // - register its start on the chosen worker
                // must be atomic to avoid that multiple threads
                // select the same worker for multiple instances of the
                // same task type.
                final NodePerformanceInfo[] tables = gossiper.getGossipCopy();
                final HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap = nodes
                        .getLocalNodeInfo();
                final Submission submission = masterQueue.getSubmission(
                        localNodeInfoMap, tables);
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
            final RunTaskMessage msg = new RunTaskMessage(node, task, taskId,
                    null);
            final boolean ok = sendPort.send(node, msg);
            if (ok) {
                submitMessageCount.add();
            } else {
                // Try to put the paste back in the tube.
                // The send port has already registered the trouble.
                masterQueue.add(msg.taskInstance);
                worker.retractTask(taskId);
            }
            changed = true;
        }
        if (changed) {
            recomputeCompletionTimes.set();
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
        boolean isnew = gossiper.registerGossip(m.update, m.update.source);
        if (isnew) {
            // TODO: can this be moved to the gossiper?
            recomputeCompletionTimes.set();
        }
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
    @Override
    void handleTaskResult(RunTaskMessage message, Object result, double runMoment) {
        final double taskCompletionMoment = Utils.getPreciseTime();

        final JobType type = message.taskInstance.type;
        taskResultMessageCount.add();

        final JobType nextTaskType = jobs.getNextTaskType(type);
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
        } else {
            // There is a next step to take.
            final JobInstance nextTask = new JobInstance(
                    message.taskInstance.jobInstance, nextTaskType, result,
                    null);
            submit(nextTask);
        }

        // Update statistics.
        final double computeInterval = taskCompletionMoment - runMoment;
        final double averageComputeTime = workerQueue.countTask(type,
                computeInterval);
        gossiper.setComputeTime(type, averageComputeTime);
        runningTasks.down();
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final double queueInterval = runMoment - message.arrivalMoment;
            Globals.log.reportProgress("Completed " + message.taskInstance
                    + "; queueInterval="
                    + Utils.formatSeconds(queueInterval)
                    + "; runningTasks=" + runningTasks);
        }
        final double workerDwellTime = taskCompletionMoment
                - message.arrivalMoment;
        if (traceStats) {
            final double now = (Utils.getPreciseTime() - startTime);
            System.out.println("TRACE:workerDwellTime " + type + " " + now
                    + " " + workerDwellTime);
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
            gossiper.setWorkerQueueLength(msg.taskInstance.type, length);
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
        if (recomputeCompletionTimes.getAndReset()) {
            double masterQueueIntervals[] = null;
            if (!Settings.IGNORE_QUEUE_TIME) {
                masterQueueIntervals = masterQueue.getQueueIntervals();
            }
            final HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap = nodes
                    .getLocalNodeInfo();
            gossiper.recomputeCompletionTimes(masterQueueIntervals, jobs,
                    localNodeInfoMap);
        }
        super.updateAdministration();
    }

    @Override
    protected void registerNewGossipHasArrived() {
        recomputeCompletionTimes.set();
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
        Globals.log
                .reportInternalError("Received an ant trail, while this node does Q routing");
    }
}
