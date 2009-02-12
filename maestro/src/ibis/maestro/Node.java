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
    static final IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.MEMBERSHIP_UNRELIABLE,
            IbisCapabilities.ELECTIONS_STRICT);

    protected final PacketSendPort sendPort;

    private final PacketUpcallReceivePort receivePort;

    protected final double startTime;

    private double stopTime = 0;

    private static final String MAESTRO_ELECTION_NAME = "maestro-election";

    private final RegistryEventHandler registryEventHandler;

    private static final int numberOfProcessors = Runtime.getRuntime()
            .availableProcessors();

    private static final int workThreadCount = numberOfProcessors
            + Settings.EXTRA_WORK_THREADS;

    private final WorkThread workThreads[] = new WorkThread[workThreadCount];

    private final Terminator terminator;

    protected final Gossiper gossiper;

    protected final MessageQueue outgoingMessageQueue = new MessageQueue();

    private final CompletedJobJist completedJobList = new CompletedJobJist();

    private IbisIdentifier maestro = null;

    /** The list of running jobs with their completion listeners. */
    private final RunningJobs runningJobList = new RunningJobs();

    /** The list of nodes we know about. */
    protected final NodeList nodes;

    protected final IbisSet deadNodes = new IbisSet();

    private boolean isMaestro;

    protected final boolean traceStats;

    protected final MasterQueue masterQueue;

    protected final WorkerQueue workerQueue;

    protected long nextJobId = 0;

    private final UpDownCounter idleProcessors = new UpDownCounter(
            -Settings.EXTRA_WORK_THREADS); // Yes, we start with a negative

    // number of idle processors.
    protected Counter submitMessageCount = new Counter();

    private final Counter jobReceivedMessageCount = new Counter();

    private final Counter aggregateResultMessageCount = new Counter();

    private final Counter jobFailMessageCount = new Counter();

    private long overheadDuration = 0L;

    private final Flag enableRegistration = new Flag(false);

    private final Flag stopped = new Flag(false);

    protected UpDownCounter runningJobCount = new UpDownCounter(0);

    protected final JobList jobs;

    private final Flag doUpdateRecentMasters = new Flag(false);

    private final Flag recomputeCompletionTimes = new Flag(false);

    /**
     * This object only exists to lock the critical section in drainMasterQueue,
     * and prevent that two threads select the same next job to submit to a
     * worker.
     */
    private final Flag drainLock = new Flag(false);

    protected final RecentMasterList recentMasterList = new RecentMasterList();

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
            // TODO: can we do something useful with the poolClosed() signal?
        }

        @Override
        public void poolTerminated(IbisIdentifier arg0) {
            // TODO: can we do something useful with the poolTerminated()
            // signal?

        }
    }

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
    @SuppressWarnings("synthetic-access")
    public Node(JobList jobs, boolean runForMaestro)
            throws IbisCreationFailedException, IOException {
        final Properties ibisProperties = new Properties();

        this.jobs = jobs;
        final JobType supportedTypes[] = jobs.getSupportedJobTypes();
        final JobType[] allTypes = jobs.getAllTypes();
        Globals.allJobTypes = allTypes;
        Globals.supportedJobTypes = supportedTypes;
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
        nodes.registerNode(localIbis.identifier(), true);
        final Registry registry = localIbis.registry();
        if (runForMaestro) {
            final IbisIdentifier m = registry.elect(MAESTRO_ELECTION_NAME);
            isMaestro = m.equals(localIbis.identifier());
            if (isMaestro) {
                enableRegistration.set(); // We're maestro, we're allowed to
                // register with others.
            }
        } else {
            isMaestro = false;

        }
        Globals.log.reportProgress("Started ibis " + localIbis.identifier()
                + ": isMaestro=" + isMaestro);
        if (!isMaestro && supportedTypes.length == 0) {
            Globals.log
                    .reportProgress("This node does not support any types, and isn't the maestro. Stopping");
            stopped.set();
        }
        sendPort = new PacketSendPort(this, localIbis.identifier());
        terminator = buildTerminator();
        receivePort = new PacketUpcallReceivePort(localIbis,
                Globals.receivePortName, this);
        traceStats = System.getProperty("ibis.maestro.traceWorkerStatistics") != null;
        gossiper = new Gossiper(sendPort, isMaestro(), jobs);
        startTime = Utils.getPreciseTime();
        recentMasterList.register(localIbis.identifier());
        startThreads();
        if( Settings.traceNodes ){
            Globals.log.log("Started a Maestro node");
        }
    }

    protected void startThreads() {
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
        final ArrayList<JobInstance> orphans = nodes.removeNode(theIbis);
        if (maestro != null && theIbis.equals(maestro)) {
            Globals.log.reportProgress("The maestro has left; stopping..");
            setStopped();
        } else if (theIbis.equals(Globals.localIbis.identifier())) {
            // The registry has declared us dead. We might as well stop.
            Globals.log
                    .reportProgress("This node has been declared dead, stopping..");
            setStopped();
        }
        if (!stopped.isSet()) {
            for (final JobInstance ti : orphans) {
                ti.setOrphan();
            }
            masterQueue.add(orphans);
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
        final JobInstanceInfo job = runningJobList.remove(id);
        if (job != null) {
            job.listener.jobCompleted(this, id.userId, result);
        }
    }

    void addRunningJob(JobInstanceIdentifier id, JobInstance jobInstance,
            JobSequence job, JobCompletionListener listener) {
        runningJobList.add(new JobInstanceInfo(id, jobInstance, job, listener));
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
     * Do all updates of the node adminstration that we can.
     * 
     */
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
        drainCompletedJobList();
        drainOutgoingMessageQueue();
        restartLateJobs();
        drainMasterQueue();
        nodes.checkDeadlines(Utils.getPreciseTime());
    }

    /** Print some statistics about the entire worker run. */
    synchronized void printStatistics(PrintStream s) {
        if (stopTime < startTime) {
            Globals.log.reportError("printStatistics(): Node didn't stop yet");
            stopTime = Utils.getPreciseTime();
        }
        s.printf("# work threads  = %5d\n", workThreads.length);
        nodes.printStatistics(s);
        jobs.printStatistics(s);
        s.printf("submit       messages:   %5d sent\n", submitMessageCount
                .get());
        s.printf("job received messages:   %5d sent\n",
                jobReceivedMessageCount.get());
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
        gossiper.printStatistics(s);
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
    protected boolean sendJobResultMessage(JobInstanceIdentifier id,
            Object result) {
        if (deadNodes.contains(id.ibis)) {
            // We say it has been delivered to avoid error recovery.
            // Think of this as an optimization.
            return true;
        }
        final Message msg = new JobResultMessage(id, result);
        aggregateResultMessageCount.add();
        return sendPort.send(id.ibis, msg);
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
    protected void postJobReceivedMessage(IbisIdentifier master, long id) {
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
        final Message msg = new JobFailMessage(jobId);
        jobFailMessageCount.add();
        return sendPort.send(ibis, msg);
    }

    /**
     * A worker has sent use a completion message for a job. Process it.
     * 
     * @param result
     *            The message.
     */
    protected void handleJobCompletedMessage(JobCompletedMessage result) {
        if (Settings.traceNodeProgress) {
            Globals.log
                    .reportProgress("Received a job completed message "
                            + result);
        }
        final JobInstance job = nodes.registerJobCompleted(result);
        if (job != null) {
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
    private void handleJobFailMessage(JobFailMessage msg) {
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Received a job failed message "
                    + msg);
        }
        final JobInstance failedJob = nodes.registerJobFailed(msg.source,
                msg.id);
        Globals.log.reportError("Node " + msg.source
                + " failed to execute job with id " + msg.id
                + "; node will no longer get jobs of this type");
        masterQueue.add(failedJob);
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
     * Handles message <code>msg</code> from worker.
     * 
     * @param msg
     *            The message we received.
     */
    @Override
    public void messageReceived(Message msg) {
        handleMessage(msg);
    }

    /**
     * A node has sent us a gossip message, handle it.
     * 
     * @param m
     *            The gossip message.
     */
    protected void handleGossipMessage(GossipMessage m) {
        boolean changed = gossiper.registerGossipMessage(m);
        if (changed) {
            registerNewGossipHasArrived();
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    /** Handle the given message. */
    void handleMessage(Message msg) {
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
        } else if (msg instanceof JobFailMessage) {
            handleJobFailMessage((JobFailMessage) msg);
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

    protected void waitForWorkThreadsToTerminate() {
        if (isMaestro) {
            for (final WorkThread t : workThreads) {
                if (Settings.traceNodes) {
                    Globals.log
                            .reportProgress("Waiting for termination of thread "
                                    + t);
                }
                Utils.waitToTerminate(t);
            }
        }
        gossiper.setStopped();
    }

    private void restartLateJobs() {
        if (masterQueue.isEmpty() && workerQueue.isEmpty() && !stopped.isSet()) {
            final JobInstance job = runningJobList.getLateJob();
            if (job != null) {
                Globals.log.reportProgress("Resubmitted late job " + job);
                masterQueue.add(job);
            }
        }

    }

    void submit(JobInstance job) {
        masterQueue.add(job);
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
            final ParallelJob mrt = (ParallelJob) job;
            final ParallelJobHandler handler = new ParallelJobHandler(this, mrt,
                    message, runMoment);
            mrt.map(input, handler);
            handler.start();
        } else if (job instanceof JobSequence ) {
            Globals.log.reportInternalError( "JobSequence should be handled" );
        } else if (job instanceof AlternativesJob) {
            Globals.log
                    .reportInternalError("AlternativesJob should have been selected by the master "
                            + job);
        } else {
            Globals.log
                    .reportInternalError("Don't know what to do with a jbo of type "
                            + job.getClass());
        }
    }

    /**
     * Wait until the master queue has drained to a reasonable level.
     */
    void waitForRoom() {
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
                    message = getWork();
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
                    final JobType type = message.jobInstance.type;
                    final Job job = jobs.getJob(type);

                    runningJobCount.up();
                    if (Settings.traceNodeProgress) {
                        final double queueInterval = runMoment
                                - message.arrivalMoment;
                        Globals.log.reportProgress("Worker: handed out task "
                                + message + " of type " + type
                                + "; it was queued for "
                                + Utils.formatSeconds(queueInterval)
                                + "; there are now " + runningJobCount
                                + " running tasks");
                    }
                    final Object input = message.jobInstance.input;
                    threadOverhead += Utils.getPreciseTime() - overheadStart;
                    executeJob(message, job, input, runMoment);
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

    protected void failNode(RunJobMessage message, Throwable t) {
        final JobType type = message.jobInstance.type;
        Globals.log.reportError("Node fails for type " + type);
        t.printStackTrace(Globals.log.getPrintStream());
        final boolean allFailed = workerQueue.failJob(type);
        sendJobFailMessage(message.source, message.jobId);
        if (allFailed && !isMaestro) {
            setStopped();
        }
        gossiper.failJob(message.jobInstance.type);
    }

    /**
     * Writes the given progress message to the logger.
     * 
     * @param msg
     *            The message to write.
     */
    public void reportProgress(String msg) {
        Globals.log.reportProgress(msg);
    }

    /**
     * Writes the given error message to the logger.
     * 
     * @param msg
     *            The message to write.
     */
    public void reportError(String msg) {
        Globals.log.reportError(msg);
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

    protected RunJobMessage getWork() {
        return workerQueue.remove(gossiper);
    }

    /**
     * Given an input and a list of possible jobs to execute, submit this input
     * as a job with the best promised completion time. If
     * <code>submitIfBusy</code> is set, also consider jobs where all workers
     * are currently busy.
     * 
     * @param input
     *            The input of the job.
     *            @param userId The user-supplied id of the job.
     * @param submitIfBusy
     *            If set, also consider jobs for which all workers are currently
     *            busy.
     * @param listener
     *            The completion listener for this job.
     * @param choices
     *            The list of job choices.
     * @return <code>true</code> if the job could be submitted.
     */
    public boolean submit(Object input, Serializable userId, boolean submitIfBusy, JobCompletionListener listener,
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
            
                        types[ix] = job.getFirstJobType();
                    }
                    final HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap = nodes
                            .getLocalNodeInfo();
                    choice = gossiper.selectFastestJob(types, submitIfBusy,
                            localNodeInfoMap);
                    if (choice < 0) {
                        // Couldn't submit the job.
                        return false;
                    }
                }
                final JobSequence job = choices[choice];
                job.submit(this, input, userId, listener);
                return true;
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
        return new Node(jobs, goForMaestro);
    }

    /**
     * Writes the given error message about an internal inconsistency in the
     * program to the logger.
     * 
     * @param msg
     *            The message to write.
     */
    public void reportInternalError(String msg) {
        Globals.log.reportInternalError(msg);
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
        nodes.registerNode(theIbis, local);
        if (!local) {
            gossiper.registerNode(theIbis);
        }
    }

    /** On a locked queue, try to send out as many jobs as we can. */
    protected void drainMasterQueue() {
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
                job = submission.task;
                worker = nodes.get(node);
                jobId = nextJobId++;
    
                worker.registerJobStart(job, jobId,
                        submission.predictedDuration);
            }
            if (Settings.traceMasterQueue || Settings.traceSubmissions) {
                Globals.log.reportProgress("Submitting job " + job + " to "
                        + node);
            }
            final RunJobMessage msg = new RunJobMessage(node, job, jobId);
            final boolean ok = sendPort.send(node, msg);
            if (ok) {
                submitMessageCount.add();
            } else {
                // Try to put the paste back in the tube.
                // The send port has already registered the trouble.
                masterQueue.add(msg.jobInstance);
                worker.retractJob(jobId);
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
     * @param message
     *            The job that was run.
     * @param result
     *            The result of the job.
     * @param runMoment
     *            The moment the job was started.
     */
    protected void handleJobResult(RunJobMessage message, Object result, double runMoment) {
        final double jobCompletionMoment = Utils.getPreciseTime();
    
        final JobType type = message.jobInstance.type;
    
        final JobType nextJobType = jobs.getNextJobType(type);
        if (nextJobType == null) {
            // This was the final step. Report back the result.
            final JobInstanceIdentifier identifier = message.jobInstance.jobInstance;
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
                    message.jobInstance.jobInstance, nextJobType, result);
            submit(nextTask);
        }
    
        // Update statistics.
        final double computeInterval = jobCompletionMoment - runMoment;
        final double averageComputeTime = workerQueue.countTask(type,
                computeInterval);
        gossiper.setComputeTime(type, averageComputeTime);
        runningJobCount.down();
        if (Settings.traceNodeProgress || Settings.traceRemainingJobTime) {
            final double queueInterval = runMoment - message.arrivalMoment;
            Globals.log.reportProgress("Completed " + message.jobInstance
                    + "; queueInterval="
                    + Utils.formatSeconds(queueInterval)
                    + "; runningTasks=" + runningJobCount);
        }
        final double workerDwellTime = jobCompletionMoment
                - message.arrivalMoment;
        if (traceStats) {
            final double now = (Utils.getPreciseTime() - startTime);
            System.out.println("TRACE:workerDwellTime " + type + " " + now
                    + " " + workerDwellTime);
        }
        if (!deadNodes.contains(message.source)) {
            final Message msg = new JobCompletedMessage(message.jobId,
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
    protected void handleRunJobMessage(RunJobMessage msg) {
        final IbisIdentifier source = msg.source;
        final boolean isDead = nodes.registerAsCommunicating(source);
        if (!isDead && !source.equals(Globals.localIbis.identifier())) {
            recentMasterList.register(source);
        }
        doUpdateRecentMasters.set();
        postJobReceivedMessage(source, msg.jobId);
        final int length = workerQueue.add(msg);
        if (gossiper != null) {
            gossiper.setWorkerQueueLength(msg.jobInstance.type, length);
        }
    }

    protected void registerNewGossipHasArrived() {
        recomputeCompletionTimes.set();
    }
}
