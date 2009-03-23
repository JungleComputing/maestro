package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.Registry;
import ibis.ipl.RegistryEventHandler;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 * A node in the Maestro dataflow network.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public final class Node extends Thread implements PacketReceiveListener {
    private static final IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.MEMBERSHIP_UNRELIABLE,
            IbisCapabilities.ELECTIONS_STRICT);

    private final PacketSendPort sendPort;

    private final PacketUpcallReceivePort receivePort;

    private final double startTime;

    private double stopTime = 0;

    private static final String MAESTRO_ELECTION_NAME = "maestro-election";

    private final RegistryEventHandler registryEventHandler;

    private static final int numberOfProcessors = Runtime.getRuntime()
    .availableProcessors();

    private static final int workThreadCount = numberOfProcessors
    + Settings.EXTRA_WORK_THREADS;

    private final WorkThread workThreads[] = new WorkThread[workThreadCount];

    private final ParallelJobHandler parallelJobHandler = new ParallelJobHandler( this );

    private final Terminator terminator;

    private final Gossiper gossiper;

    private final MessageQueue outgoingMessageQueue = new MessageQueue();

    private final CompletedJobList completedJobList = new CompletedJobList();

    private IbisIdentifier maestro = null;

    /** The list of running jobs with their completion listeners. */
    private final RunningJobs runningJobList = new RunningJobs();

    /** The list of nodes we know about. */
    private final NodeList nodes;

    private final IbisSet deadNodes = new IbisSet();

    private final boolean isMaestro;

    private final boolean traceStats;

    private final MasterQueue masterQueue;

    private final WorkerQueue workerQueue;

    private long nextJobId = 0;

    private final UpDownCounter idleProcessors = new UpDownCounter(
            -Settings.EXTRA_WORK_THREADS); // Yes, we start with a negative

    private final Counter submitMessageCount = new Counter();

    private final Counter jobReceivedMessageCount = new Counter();

    private final Counter jobCompletedMessageCount = new Counter();

    private final Counter aggregateResultMessageCount = new Counter();

    private final Counter jobFailMessageCount = new Counter();

    private long overheadDuration = 0L;

    private final Flag enableRegistration = new Flag(false);

    private final Flag stopped = new Flag(false);

    private final UpDownCounter runningJobCount = new UpDownCounter(0);

    private final JobList jobs;

    private final Flag doUpdateRecentMasters = new Flag(false);

    private final Flag recomputeCompletionTimes = new Flag(false);

    /**
     * This object only exists to lock the critical section in drainMasterQueue,
     * and prevent that two threads select the same next job to submit to a
     * worker.
     */
    private final Flag drainLock = new Flag(false);

    private final RecentMasterList recentMasterList = new RecentMasterList();

    private final Counter updateMessageCount = new Counter();

    private final class NodeRegistryEventHandler implements
    RegistryEventHandler {
        /**
         * A new Ibis joined the computation.
         * 
         * @param theIbis
         *            The ibis that joined the computation.
         */
        @Override
        public void joined(IbisIdentifier theIbis) {
            registerIbisJoined(theIbis);
        }

        /**
         * An ibis has died.
         * 
         * @param theIbis
         *            The ibis that died.
         */
        @Override
        public void died(IbisIdentifier theIbis) {
            Globals.log.reportProgress("Ibis " + theIbis + " has died");
            registerIbisLeft(theIbis);
        }

        /**
         * An ibis has explicitly left the computation.
         * 
         * @param theIbis
         *            The ibis that left.
         */
        @Override
        public void left(IbisIdentifier theIbis) {
            registerIbisLeft(theIbis);
        }

        /**
         * The results of an election are known.
         * 
         * @param name
         *            The name of the election.
         * @param theIbis
         *            The ibis that was elected.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public void electionResult(String name, IbisIdentifier theIbis) {
            if (name.equals(MAESTRO_ELECTION_NAME) && theIbis != null) {
                Globals.log.reportProgress("Ibis " + theIbis
                        + " was elected maestro");
                maestro = theIbis;
            }
        }

        @Override
        public void gotSignal(String arg0, IbisIdentifier arg1) {
            // Not interested.
        }

        @Override
        public void poolClosed() {
            // TODO: can we do something with the poolClosed() signal?
        }

        @Override
        public void poolTerminated(IbisIdentifier arg0) {
            // TODO: can we do something with the poolTerminated() signal?

        }
    }

    /**
     * Constructs a new Maestro node using the given list of jobs. Optionally
     * try to get elected as maestro.
     * @param runForMaestro
     *            If true, try to get elected as maestro.
     * @param jobs
     *            The jobs that should be supported in this node.
     * 
     * @throws IbisCreationFailedException
     *             Thrown if for some reason we cannot create an ibis.
     * @throws IOException
     *             Thrown if for some reason we cannot communicate.
     */
    @SuppressWarnings("synthetic-access")
    private Node(boolean runForMaestro, JobList jobs)
    throws IbisCreationFailedException, IOException {
        final Properties ibisProperties = new Properties();

        this.jobs = jobs;
        //final JobType supportedTypes[] = jobs.getSupportedJobTypes();
        final JobType[] allTypes = jobs.getAllTypes();
        masterQueue = new MasterQueue(allTypes);
        workerQueue = new WorkerQueue(jobs);
        nodes = new NodeList(workerQueue);
        registryEventHandler = new NodeRegistryEventHandler();
        Globals.localIbis = IbisFactory.createIbis(ibisCapabilities,
                ibisProperties, true, registryEventHandler,
                PacketSendPort.portType, PacketUpcallReceivePort.portType);
        final Ibis localIbis = Globals.localIbis;
        if (Settings.traceNodes) {
            Globals.log.reportProgress("Created ibis " + localIbis);
        }
        final IbisIdentifier myIbis = localIbis.identifier();
        nodes.registerNode(myIbis, true,jobs.getTypeCount());
        final Registry registry = localIbis.registry();
        if (runForMaestro) {
            final IbisIdentifier m = registry.elect(MAESTRO_ELECTION_NAME);
            isMaestro = m.equals(myIbis);
            if (isMaestro) {
                enableRegistration.set(); // We're maestro, we're allowed to
                // register with others.
            }
        } else {
            isMaestro = false;

        }
        Globals.log.reportProgress("Started ibis " + myIbis
                + ": isMaestro=" + isMaestro);
        sendPort = new PacketSendPort(this, myIbis);
        terminator = buildTerminator();
        receivePort = new PacketUpcallReceivePort(localIbis,
                Globals.receivePortName, this);
        traceStats = System.getProperty("ibis.maestro.traceWorkerStatistics") != null;
        gossiper = new Gossiper(sendPort, isMaestro(), jobs,myIbis);
        startTime = Utils.getPreciseTime();
        recentMasterList.register(myIbis);
        startThreads();
        if( Settings.traceNodes ){
            Globals.log.log("Started a Maestro node");
        }
    }

    private void startThreads() {
        gossiper.start();
        for (int i = 0; i < workThreads.length; i++) {
            final WorkThread t = new WorkThread(this);
            workThreads[i] = t;
            t.start();
        }
        start();
        final Registry registry = Globals.localIbis.registry();
        registry.enableEvents();
    }

    private Terminator buildTerminator() {
        if (!isMaestro) {
            // We only run a terminator on the maestro.
            return null;
        }
        final String terminatorStartQuotumString = System
        .getProperty("ibis.maestro.terminatorStartQuotum");
        final String terminatorNodeQuotumString = System
        .getProperty("ibis.maestro.terminatorNodeQuotum");
        final String terminatorInitialSleepString = System
        .getProperty("ibis.maestro.terminatorInitialSleepTime");
        final String terminatorSleepString = System
        .getProperty("ibis.maestro.terminatorSleepTime");
        try {
            if (terminatorInitialSleepString == null) {
                return null;
            }
            final long initialSleep = Long
            .parseLong(terminatorInitialSleepString);
            double startQuotum;
            double nodeQuotum;
            long sleep;
            if (terminatorStartQuotumString == null) {
                startQuotum = Settings.DEFAULT_TERMINATOR_START_QUOTUM;
            } else {
                startQuotum = Double.parseDouble(terminatorStartQuotumString);
            }
            if (terminatorNodeQuotumString == null) {
                nodeQuotum = Settings.DEFAULT_TERMINATOR_NODE_QUOTUM;
            } else {
                nodeQuotum = Double.parseDouble(terminatorNodeQuotumString);
            }
            if (terminatorSleepString == null) {
                sleep = Settings.DEFAULT_TERMINATOR_SLEEP;
            } else {
                sleep = Long.parseLong(terminatorSleepString);
            }
            final Terminator t = new Terminator(startQuotum, nodeQuotum,
                    initialSleep, sleep);
            t.start();
            Globals.log.reportProgress("Started terminator");
            return t;
        } catch (final Throwable e) {
            Globals.log.reportInternalError("Bad terminator specification: "
                    + e.getLocalizedMessage());
        }
        return null;
    }

    /**
     * Start this thread. Do not invoke this method, it is already invoked in
     * the constructor of this node.
     */
    @Override
    public void start() {
        receivePort.enable(); // We're open for business.
        super.start(); // Start the thread
    }

    /**
     * Set this node to the stopped state. This does not mean that the node
     * stops immediately, but it does mean the master and worker try to wind
     * down the work.
     */
    public void setStopped() {
        if (Settings.traceNodes) {
            Globals.log.reportProgress("Set node to stopped state");
        }
        stopped.set();
        masterQueue.clear();
        runningJobList.clear();
        workerQueue.clear();
        kickAllWorkers();
    }

    /**
     * Wait for this node to finish.
     */
    public void waitToTerminate() {
        if (Settings.traceNodes) {
            Globals.log.reportProgress("Waiting for node to stop");
        }

        stopped.waitUntilSet();
        synchronized (this) {
            stopTime = Utils.getPreciseTime();
        }
        if (Settings.traceNodes) {
            Globals.log.reportProgress("Node has terminated");
        }
        try {
            Globals.localIbis.end();
        } catch (final IOException x) {
            // Nothing we can do about it.
        }
        printStatistics(Globals.log.getPrintStream());
        //Globals.log.close();
    }

    /**
     * Returns true iff this node is a maestro.
     * 
     * @return True iff this node is a maestro.
     */
    public boolean isMaestro() {
        return isMaestro;
    }

    private synchronized void kickAllWorkers() {
        this.notifyAll();
    }

    /**
     * Registers the ibis with the given identifier as one that has left the
     * computation.
     * 
     * @param theIbis
     *            The ibis that has left.
     */
    protected void registerIbisLeft(IbisIdentifier theIbis) {
        gossiper.removeNode(theIbis);
        recentMasterList.remove(theIbis);
        if (terminator != null) {
            terminator.removeNode(theIbis);
        }
        deadNodes.add(theIbis);
        if (maestro != null && theIbis.equals(maestro)) {
            Globals.log.reportProgress("The maestro has left; stopping..");
            setStopped();
        } else if (theIbis.equals(Globals.localIbis.identifier())) {
            // The registry has declared us dead. We might as well stop.
            Globals.log
            .reportProgress("This node has been declared dead, stopping..");
            setStopped();
        }
        final ArrayList<JobInstance> orphans = nodes.removeNode(theIbis);
        if (!stopped.isSet()) {
            for (final JobInstance ti : orphans) {
                ti.setOrphan();
            }
            masterQueue.add(jobs,orphans);
        }
    }

    /**
     * Report the completion of the job with the given identifier.
     * 
     * @param id
     *            The job that has been completed.
     * @param result
     *            The job result.
     */
    private void reportCompletion(JobInstanceIdentifier id, Object result) {
        final SubmittedJobInfo job = runningJobList.remove(id);
        if (job != null) {
            job.listener.jobCompleted(this, id.userId, result);
        }
    }

    /**
     * This ibis was reported as 'may be dead'. Try not to communicate with it.
     * 
     * @param theIbis
     *            The ibis that may be dead.
     */
    void setSuspect(IbisIdentifier theIbis) {
        try {
            Globals.localIbis.registry().assumeDead(theIbis);
        } catch (final IOException e) {
            // Nothing we can do about it.
        }
        nodes.setSuspect(theIbis);
    }

    private void drainCompletedJobList() {
        while (true) {
            final CompletedJob j = completedJobList.poll();
            if (j == null) {
                break;
            }
            reportCompletion(j.job, j.result);
        }
    }

    private void drainOutgoingMessageQueue() {
        while (true) {
            final QueuedMessage msg = outgoingMessageQueue.getNext();
            if (msg == null) {
                return;
            }
            if (!deadNodes.contains(msg.destination)) {
                sendPort.send(msg.destination, msg.msg);
            }
        }
    }

    /**
     * Do all updates of the node administration that we can.
     * 
     */
    private void updateAdministration() {
        if (doUpdateRecentMasters.getAndReset()) {
            updateRecentMasters();
        }
        if (recomputeCompletionTimes.getAndReset()) {
            double masterQueueIntervals[] = null;
            if (!Settings.IGNORE_QUEUE_TIME) {
                masterQueueIntervals = masterQueue.getQueueIntervals();
            }
            final HashMap<IbisIdentifier, LocalNodeInfoList> localNodeInfoMap = nodes
            .getLocalNodeInfo();
            gossiper.recomputeCompletionTimes(masterQueueIntervals, jobs,
                    localNodeInfoMap);
        }
        drainCompletedJobList();
        drainOutgoingMessageQueue();
        restartLateJobs();
        drainMasterQueue();
    }

    /** Print some statistics about the entire worker run. */
    private synchronized void printStatistics(PrintStream s) {
        if (stopTime < startTime) {
            Globals.log.reportError("printStatistics(): Node didn't stop yet");
            stopTime = Utils.getPreciseTime();
        }
        s.printf("# work threads  = %5d\n", workThreads.length);
        nodes.printStatistics(s);
        s.printf("submit       messages:   %5d sent\n", submitMessageCount
                .get());
        s.printf("job received messages:   %5d sent\n",
                jobReceivedMessageCount.get());
        s.printf("job completed messages:  %5d sent\n",
                jobCompletedMessageCount.get());
        s.printf("job result   messages:   %5d sent\n", aggregateResultMessageCount
                .get());
        s.printf("job fail     messages:   %5d sent\n", jobFailMessageCount
                .get());
        if (terminator != null) {
            terminator.printStatistics(s);
        }
        sendPort.printStatistics(s, "send port");
        final double activeTime = workerQueue.getActiveTime(startTime);
        final double workInterval = stopTime - activeTime;
        workerQueue.printStatistics(s, workInterval);
        s.println("run time        = " + Utils.formatSeconds(workInterval));
        s.println("activated after = "
                + Utils.formatSeconds(activeTime - startTime));
        final double overheadPercentage = 100.0 * (overheadDuration / workInterval);
        s.println("Total overhead time = "
                + Utils.formatSeconds(overheadDuration)
                + String.format(" (%.1f%%)", overheadPercentage));
        masterQueue.printStatistics(s);
        Utils.printThreadStats(s);
        gossiper.printStatistics(s,jobs);
        s.printf("update        messages:   %5d sent\n", updateMessageCount
                .get());
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * 
     * @param id
     *            The job instance identifier.
     * @param result
     *            The result to send.
     * @return <code>true</code> if the message could be sent.
     */
    private boolean sendJobResultMessage(JobInstanceIdentifier id,
            Object result) {
        if (deadNodes.contains(id.resultNode)) {
            // We say it has been delivered to avoid error recovery.
            // Think of this as an optimization.
            return true;
        }
        final Message msg = new JobResultMessage(id, result);
        aggregateResultMessageCount.add();
        return sendPort.send(id.resultNode, msg);
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * 
     * @param id
     *            The job identifier.
     * @param result
     *            The result to send.
     */
    private void postJobReceivedMessage(IbisIdentifier master, long id) {
        final Message msg = new JobReceivedMessage(id);
        jobReceivedMessageCount.add();
        synchronized (outgoingMessageQueue) {
            outgoingMessageQueue.add(master, msg);
        }
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * 
     * @param jobId
     *            The job instance that failed.
     * @return <code>true</code> if the message could be sent.
     */
    private boolean sendJobFailMessage(IbisIdentifier ibis, long jobId) {
        if (deadNodes.contains(ibis)) {
            return false;
        }
        final Message msg = new JobFailedMessage(jobId);
        jobFailMessageCount.add();
        return sendPort.send(ibis, msg);
    }

    /**
     * A worker has sent use a completion message for a job. Process it.
     * 
     * @param result
     *            The message.
     */
    private void handleJobCompletedMessage(JobCompletedMessage result) {
        if (Settings.traceNodeProgress) {
            Globals.log
            .reportProgress("Received a job completed message "
                    + result);
        }
        final JobInstance job = nodes.registerJobCompleted(jobs,result);
        if (job != null) {
            // This was an outstanding job, remove it from our administration.
            masterQueue.removeDuplicates(job);
        }
        doUpdateRecentMasters.set();
    }

    /**
     * A worker has sent use a received message for a job. Process it.
     * 
     * @param result
     *            The message.
     */
    private void handleJobReceivedMessage(JobReceivedMessage result) {
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Received a job received message "
                    + result);
        }
        nodes.registerJobReceived(result);
    }

    /**
     * A worker has sent use a fail message for a job. Process it.
     * 
     * @param msg
     *            The status message.
     */
    private void handleJobFailMessage(JobFailedMessage msg) {
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Received a job failed message "
                    + msg);
        }
        final JobInstance failedJob = nodes.registerJobFailed(msg.source,
                msg.id);
        Globals.log.reportError("Node " + msg.source
                + " failed to execute job with id " + msg.id
                + "; node will no longer get jobs of this type");
        masterQueue.add(jobs,failedJob);
    }

    /**
     * A worker has sent use a completion message for a job. Process it.
     * 
     * @param msg
     *            The status message.
     */
    private void handleStopNodeMessage(StopNodeMessage msg) {
        Globals.log.reportProgress("Node was forced to stop by " + msg.source);
        System.exit(2);
    }

    private void handleJobResultMessage(JobResultMessage m) {
        completedJobList.add(new CompletedJob(m.job, m.result));
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * 
     * @param m
     *            The update message.
     */
    private void handleNodeUpdateMessage(UpdateNodeMessage m) {
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Received node update message " + m);
        }
        final boolean isnew = gossiper.registerGossip(m.update, m.update.source);
        if (isnew) {
            // TODO: can this be moved to the gossiper?
            recomputeCompletionTimes.set();
        }
    }

    /**
     * Handle a message containing a new job to run.
     * 
     * @param msg
     *            The message to handle.
     */
    private void handleRunJobMessage(RunJobMessage msg) {
        final IbisIdentifier source = msg.source;
        final boolean isDead = nodes.registerAsCommunicating(source);
        if (!isDead && !source.equals(Globals.localIbis.identifier())) {
            recentMasterList.register(source);
        }
        final JobType stageType = msg.jobInstance.getStageType(jobs);
        final int length = workerQueue.add(stageType,msg);
        postJobReceivedMessage(source, msg.jobId);
        if (gossiper != null) {
            final boolean changed = gossiper.setWorkerQueueLength(stageType, length);
            if( changed ) {
                doUpdateRecentMasters.set();
            }
        }
    }

    /**
     * A node has sent us a gossip message, handle it.
     * 
     * @param m
     *            The gossip message.
     */
    private void handleGossipMessage(GossipMessage m) {
        final boolean changed = gossiper.registerGossipMessage(m);
        if (changed) {
            recomputeCompletionTimes.set();
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    /**
     * Handles message <code>msg</code> from worker.
     * 
     * @param msg
     *            The message we received.
     */
    @Override
    public void messageReceived(Message msg) {
        handleMessage(msg);
    }

    /** Handle the given message. */
    private void handleMessage(Message msg) {
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Received message " + msg);
        }
        if (msg instanceof UpdateNodeMessage) {
            handleNodeUpdateMessage((UpdateNodeMessage) msg);
        } else if (msg instanceof GossipMessage) {
            handleGossipMessage((GossipMessage) msg);
        } else if (msg instanceof JobCompletedMessage) {
            handleJobCompletedMessage((JobCompletedMessage) msg);
        } else if (msg instanceof JobReceivedMessage) {
            handleJobReceivedMessage((JobReceivedMessage) msg);
        } else if (msg instanceof JobResultMessage) {
            handleJobResultMessage((JobResultMessage) msg);
        } else if (msg instanceof RunJobMessage) {
            handleRunJobMessage((RunJobMessage) msg);
        } else if (msg instanceof JobFailedMessage) {
            handleJobFailMessage((JobFailedMessage) msg);
        } else if (msg instanceof StopNodeMessage) {
            handleStopNodeMessage((StopNodeMessage) msg);
        } else {
            Globals.log
            .reportInternalError("the node should handle message of type "
                    + msg.getClass());
        }
        synchronized (this) {
            this.notifyAll();
        }
    }

    private void restartLateJobs() {
        if (masterQueue.isEmpty() && workerQueue.isEmpty() && !stopped.isSet()) {
            final JobInstance job = runningJobList.getLateJob();
            if (job != null) {
                Globals.log.reportProgress("Resubmitted late job " + job);
                masterQueue.add(jobs,job);
            }
        }

    }

    private boolean keepRunning() {
        if (!stopped.isSet()) {
            return true;
        }
        if (runningJobCount.isAbove(0)) {
            return true;
        }
        return false;
    }

    /**
     * @param source
     * @param jobId
     */
    private void sendJobCompletedMessage(IbisIdentifier source, long jobId) {
        final Message msg = new JobCompletedMessage(jobId);
        boolean ok = sendPort.send(source, msg);
    
    
        if (ok) {
            jobCompletedMessageCount.add();
        }
        else {
            // Could not send the result message. We're desperate.
            // First simply try again.
            ok = sendPort.send(source, msg);
            if (ok) {
                jobCompletedMessageCount.add();
            }
            else {
                // Unfortunately, that didn't work.
                // TODO: think up another way to recover from a failed
                // result report.
                Globals.log
                .reportError("Failed to send job completed message to "
                        + source);
            }
        }
    }

    /**
     * @param message
     *            The job that was run.
     * @param result
     *            The result of the job.
     * @param runMoment
     *            The moment the job was started.
     */
    protected void handleJobResult(RunJobMessage message, Object result, double runMoment) {
        final double jobCompletionMoment = Utils.getPreciseTime();
        final JobInstance jobInstance = message.jobInstance;
        final JobType todoList[] = jobs.getTodoList(jobInstance.overallType);
        final int stage = jobInstance.stageNumber;
        final JobType completedStageType = todoList[stage];
        final int nextStageNumber = stage+1;

        if (nextStageNumber>=todoList.length) {
            // This was the final step. Report back the result.
            final JobInstanceIdentifier identifier = jobInstance.jobInstance;
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
            final JobInstance nextJob = new JobInstance(
                    jobInstance.jobInstance, result,
                    jobInstance.overallType,nextStageNumber
            );
            masterQueue.add(jobs,nextJob);
        }

        // Update statistics.
        final double computeInterval = jobCompletionMoment - runMoment;
        final double averageComputeTime = workerQueue.countJob(completedStageType,
                computeInterval);
        gossiper.setComputeTime(completedStageType, averageComputeTime);
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final double queueInterval = runMoment - message.arrivalMoment;
            Globals.log.reportProgress("Completed " + jobInstance
                    + "; queueInterval="
                    + Utils.formatSeconds(queueInterval)
                    + "; runningJobs=" + runningJobCount);
        }
        final double workerDwellTime = jobCompletionMoment
        - message.arrivalMoment;
        if (traceStats) {
            final double now = (Utils.getPreciseTime() - startTime);
            System.out.println("TRACE:workerDwellTime " + completedStageType + " " + now
                    + " " + workerDwellTime);
        }
        IbisIdentifier source = message.source;
        if (!deadNodes.contains(source)&& !jobs.isParallelJobType(completedStageType)) {
            // If the master node isn't dead, tell it this job
            // is completed.
            long jobId = message.jobId;
            sendJobCompletedMessage(source, jobId);
        }
        doUpdateRecentMasters.set();
    }

    private void executeJob(RunJobMessage message, Job job, Object input,
            double runMoment) {
        if (job instanceof AtomicJob) {
            final AtomicJob at = (AtomicJob) job;
            try {
                final Object result = at.run(input);
                handleJobResult(message, result, runMoment);
            } catch (final JobFailedException x) {
                failNode(message, x);
            }
        } else if (job instanceof ParallelJob) {
            final ParallelJob parallelJob = (ParallelJob) job;
            final ParallelJobContext context = new ParallelJobContext(message, runMoment);
            final ParallelJobInstance jobInstance = parallelJob.createInstance(context);
            System.out.println( "Splitting parallel job " + message.jobInstance );
            jobInstance.split(input, parallelJobHandler);
            if( jobInstance.resultIsReady() ){
                final Object result = jobInstance.getResult();
                handleJobResult(message,result, runMoment);
            }
            sendJobCompletedMessage(message.source,  message.jobId);
        } else if (job instanceof SeriesJob ) {
            Globals.log.reportInternalError( "SeriesJob " + job + " should be handled" );
        } else if (job instanceof AlternativesJob) {
            Globals.log
            .reportInternalError("AlternativesJob should have been selected by the master "
                    + job);
        } else {
            Globals.log
            .reportInternalError("Don't know what to do with a job of type "
                    + job.getClass());
        }
    }

    /**
     * Wait until the master queue has drained to a reasonable level.
     */
    private void waitForRoom() {
        while (true) {
            synchronized (this) {
                if (masterQueue.hasRoom()) {
                    return;
                }
                try {
                    this.wait(Settings.ROOM_POLL_INTERVAL);
                } catch (final InterruptedException e) {
                    // Ignore
                }
            }
        }

    }

    /** Run a work thread. Only return when we want to shut down the node. */
    void runWorkThread() {
        double overheadStart = Utils.getPreciseTime();
        long threadOverhead = 0L;
        try {
            while (keepRunning()) {
                RunJobMessage message = null;
                boolean readyForWork = false;

                updateAdministration();
                if (runningJobCount.isBelow(numberOfProcessors)) {
                    // Only try to start a new job when there are idle
                    // processors.
                    message = workerQueue.remove(jobs,gossiper);
                    readyForWork = true;
                }
                if (message == null) {
                    idleProcessors.up();
                    final long sleepTime = 20;
                    // Wait a little, there is nothing to do.
                    try {
                        if (readyForWork) {
                            if (Settings.traceWaits) {
                                Globals.log.reportProgress("Waiting for "
                                        + sleepTime
                                        + "ms for new jobs in queue");
                            }
                        }
                        synchronized (this) {
                            final double overhead = Utils.getPreciseTime()
                            - overheadStart;
                            // Measure the time we spend in this wait,
                            // and add to idle time.
                            if (keepRunning()) {
                                this.wait(sleepTime);
                            }
                            overheadStart = Utils.getPreciseTime();
                            threadOverhead += overhead;
                        }
                    } catch (final InterruptedException e) {
                        // Not interesting.
                    }
                    idleProcessors.down();
                } else {
                    // We have a job to execute.
                    final double runMoment = Utils.getPreciseTime();
                    final JobType stageType = message.jobInstance.getStageType(jobs);
                    final Job job = jobs.getJob(stageType);

                    runningJobCount.up();
                    if (Settings.traceNodeProgress) {
                        final double queueInterval = runMoment
                        - message.arrivalMoment;
                        Globals.log.reportProgress("Worker: handed out job "
                                + message + " of type " + stageType
                                + "; it was queued for "
                                + Utils.formatSeconds(queueInterval)
                                + "; there are now " + runningJobCount
                                + " running jobs");
                    }
                    final Object input = message.jobInstance.input;
                    threadOverhead += Utils.getPreciseTime() - overheadStart;
                    executeJob(message, job, input, runMoment);
                    runningJobCount.down();
                    overheadStart = Utils.getPreciseTime();
                    if (Settings.traceNodeProgress) {
                        Globals.log.reportProgress("Work thread: completed "
                                + message);
                    }
                }
            }
        } catch (final Throwable x) {
            Globals.log.reportError("Uncaught exception in worker thread: "
                    + x.getLocalizedMessage());
            x.printStackTrace(Globals.log.getPrintStream());
        }
        synchronized (this) {
            overheadDuration += threadOverhead;
        }
        kickAllWorkers(); // We're about to end this thread. Wake all other
        // threads.
    }

    private void failNode(RunJobMessage message, Throwable t) {
        final JobType stageType = message.jobInstance.getStageType(jobs);
        Globals.log.reportError("Node fails for type " + stageType);
        t.printStackTrace(Globals.log.getPrintStream());
        final boolean allFailed = workerQueue.failJob(stageType);
        sendJobFailMessage(message.source, message.jobId);
        if (allFailed && !isMaestro) {
            setStopped();
        }
        gossiper.failJob(stageType);
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
    public int waitForReadyNodes(int n, long maximalWaitTime) {
        return gossiper.waitForReadyNodes(n, maximalWaitTime);
    }

    /**
     * Builds a new identifier containing the given user identifier.
     * 
     * @param userIdentifier
     *            The user identifier to include in this identifier.
     * @return The newly constructed identifier.
     */
    private JobInstanceIdentifier buildJobInstanceIdentifier(
            Serializable userIdentifier) {
        return new JobInstanceIdentifier(userIdentifier, Globals.localIbis
                .identifier());
    }

    /**
     * Given an input and a job to execute, submit this input to the job.
     * If <code>submitIfBusy</code> is set, also submit when all workers
     * are currently busy.
     * 
     * @param input
     *            The input of the job.
     * @param userId The user-supplied id of the job.
     * @param listener
     *            The completion listener for this job.
     * @param job
     *            The job to execute.
     */
    public void submit(Object input, Serializable userId, JobCompletionListener listener,
            Job job) {
        waitForRoom();
        submitAlways(input, userId, listener, job);
    }

    /**
     * @param input
     * @param userId
     * @param listener
     * @param job
     */
    void submitAlways(Object input, Serializable userId,
            JobCompletionListener listener, Job job) {
        final JobInstanceIdentifier tii = buildJobInstanceIdentifier(userId);
        final JobType overallType = jobs.getJobType(job);
        final JobInstance jobInstance = new JobInstance(tii, input,overallType,0);
        runningJobList.add(new SubmittedJobInfo(tii, jobInstance, listener));
        masterQueue.add(jobs,jobInstance);
    }

    /**
     * @param jobs
     *            The list of jobs to support.
     * @param goForMaestro
     *            If <code>true</code>, try to become maestro.
     * @return The newly constructed node.
     * @throws IOException
     *             Thrown if there is an I/O error during the creation of this
     *             node.
     * @throws IbisCreationFailedException
     *             Thrown if for some reason the ibis of this node could not be
     *             created.
     */
    public static Node createNode(JobList jobs, boolean goForMaestro)
    throws IbisCreationFailedException, IOException {
        return new Node(goForMaestro, jobs);
    }

    /**
     * @param theIbis
     *            The ibis that has joined.
     */
    protected void registerIbisJoined(IbisIdentifier theIbis) {
        final boolean local = theIbis.equals(Globals.localIbis.identifier());

        if (!local) {
            if (terminator != null) {
                terminator.registerNode(theIbis);
            }
        }
        sendPort.registerDestination(theIbis);
        nodes.registerNode(theIbis, local,jobs.getTypeCount());
        if (!local) {
            gossiper.registerNode(theIbis);
        }
    }

    /** Try to send out as many jobs as we can. */
    private void drainMasterQueue() {
        boolean changed = false;

        if (masterQueue.isEmpty()) {
            // Nothing to do, don't bother with the gossip.
            return;
        }
        while (true) {
            NodeInfo worker;
            long jobId;
            IbisIdentifier node;
            JobInstance job;

            synchronized (drainLock) {
                // The entire operation
                // - get a submission
                // - register its start on the chosen worker
                // must be atomic to avoid that multiple threads
                // select the same worker for multiple instances of the
                // same job type.
                final NodePerformanceInfo[] tables = gossiper.getGossipCopy();
                final HashMap<IbisIdentifier, LocalNodeInfoList> localNodeInfoMap = nodes
                .getLocalNodeInfo();
                final Submission submission = masterQueue.getSubmission(
                        jobs,
                        localNodeInfoMap, tables);
                if (submission == null) {
                    break;
                }
                node = submission.worker;
                job = submission.job;
                worker = nodes.get(node);
                jobId = nextJobId++;

                worker.registerJobStart(jobs,job, jobId,
                        submission.predictedDuration);
            }
            if (Settings.traceMasterQueue || Settings.traceSubmissions) {
                Globals.log.reportProgress("Submitting job " + job + " to "
                        + node);
            }

            final RunJobMessage msg = new RunJobMessage(job, jobId);
            final boolean ok = sendPort.send(node, msg);
            if (ok) {
                submitMessageCount.add();
            } else {
                // Try to put the paste back in the tube.
                // The send port has already registered the trouble.
                masterQueue.add(jobs,msg.jobInstance);
                worker.retractJob(jobId);
            }
            changed = true;
        }
        if (changed) {
            recomputeCompletionTimes.set();
        }
    }

    private void updateRecentMasters() {
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
}
