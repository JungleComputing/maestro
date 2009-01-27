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
import java.util.Properties;

/**
 * A node in the Maestro dataflow network.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public abstract class Node extends Thread implements PacketReceiveListener {
    static final IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.MEMBERSHIP_UNRELIABLE,
            IbisCapabilities.ELECTIONS_STRICT);

    protected final PacketSendPort sendPort;

    private final PacketUpcallReceivePort receivePort;

    protected final long startTime;

    private long stopTime = 0;

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
    private final RunningJobs runningJobs = new RunningJobs();

    /** The list of nodes we know about. */
    protected final NodeList nodes;

    protected final IbisSet deadNodes = new IbisSet();

    private boolean isMaestro;

    protected final boolean traceStats;

    protected final MasterQueue masterQueue;

    protected final WorkerQueue workerQueue;

    protected long nextTaskId = 0;

    private final UpDownCounter idleProcessors = new UpDownCounter(
            -Settings.EXTRA_WORK_THREADS); // Yes, we start with a negative

    // number of idle processors.
    protected Counter submitMessageCount = new Counter();

    private final Counter taskReceivedMessageCount = new Counter();

    protected Counter taskResultMessageCount = new Counter();

    private final Counter jobResultMessageCount = new Counter();

    private final Counter taskFailMessageCount = new Counter();

    private long overheadDuration = 0L;

    private final Flag enableRegistration = new Flag(false);

    private final Flag stopped = new Flag(false);

    protected UpDownCounter runningTasks = new UpDownCounter(0);

    protected final JobList jobs;

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
        final TaskType supportedTypes[] = jobs.getSupportedTaskTypes();
        final TaskType[] allTypes = jobs.getAllTypes();
        Globals.allTaskTypes = allTypes;
        Globals.supportedTaskTypes = supportedTypes;
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
        startTime = System.nanoTime();
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
        runningJobs.clear();
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
            stopTime = System.nanoTime();
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
        if (terminator != null) {
            terminator.removeNode(theIbis);
        }
        deadNodes.add(theIbis);
        final ArrayList<TaskInstance> orphans = nodes.removeNode(theIbis);
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
            for (final TaskInstance ti : orphans) {
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
        final JobInstanceInfo job = runningJobs.remove(id);
        if (job != null) {
            job.listener.jobCompleted(this, id.userId, result);
        }
    }

    void addRunningJob(JobInstanceIdentifier id, TaskInstance taskInstance,
            Job job, JobCompletionListener listener) {
        runningJobs.add(new JobInstanceInfo(id, taskInstance, job, listener));
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
        drainCompletedJobList();
        drainOutgoingMessageQueue();
        restartLateJobs();
        drainMasterQueue();
        nodes.checkDeadlines(System.nanoTime());
    }

    /** Print some statistics about the entire worker run. */
    synchronized void printStatistics(PrintStream s) {
        if (stopTime < startTime) {
            Globals.log.reportError("printStatistics(): Node didn't stop yet");
            stopTime = System.nanoTime();
        }
        s.printf("# work threads  = %5d\n", workThreads.length);
        nodes.printStatistics(s);
        jobs.printStatistics(s);
        s.printf("submit        messages:   %5d sent\n", submitMessageCount
                .get());
        s.printf("task received messages:   %5d sent\n",
                taskReceivedMessageCount.get());
        s.printf("task result   messages:   %5d sent\n", taskResultMessageCount
                .get());
        s.printf("job result    messages:   %5d sent\n", jobResultMessageCount
                .get());
        s.printf("job fail      messages:   %5d sent\n", taskFailMessageCount
                .get());
        if (terminator != null) {
            terminator.printStatistics(s);
        }
        sendPort.printStatistics(s, "send port");
        final long activeTime = workerQueue.getActiveTime(startTime);
        final long workInterval = stopTime - activeTime;
        workerQueue.printStatistics(s, workInterval);
        s.println("run time        = " + Utils.formatNanoseconds(workInterval));
        s.println("activated after = "
                + Utils.formatNanoseconds(activeTime - startTime));
        final double overheadPercentage = 100.0 * ((double) overheadDuration / (double) workInterval);
        s.println("Total overhead time = "
                + Utils.formatNanoseconds(overheadDuration)
                + String.format(" (%.1f%%)", overheadPercentage));
        masterQueue.printStatistics(s);
        Utils.printThreadStats(s);
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
        jobResultMessageCount.add();
        return sendPort.send(id.ibis, msg);
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * 
     * @param id
     *            The task identifier.
     * @param result
     *            The result to send.
     */
    protected void postTaskReceivedMessage(IbisIdentifier master, long id) {
        final Message msg = new TaskReceivedMessage(id);
        taskReceivedMessageCount.add();
        synchronized (outgoingMessageQueue) {
            outgoingMessageQueue.add(master, msg);
        }
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * 
     * @param taskId
     *            The task instance that failed.
     * @return <code>true</code> if the message could be sent.
     */
    private boolean sendTaskFailMessage(IbisIdentifier ibis, long taskId) {
        if (deadNodes.contains(ibis)) {
            return false;
        }
        final Message msg = new TaskFailMessage(taskId);
        taskFailMessageCount.add();
        return sendPort.send(ibis, msg);
    }

    protected abstract void updateRecentMasters();

    /**
     * Handle a message containing a new task to run.
     * 
     * @param msg
     *            The message to handle.
     */
    protected abstract void handleRunTaskMessage(RunTaskMessage msg);

    /**
     * A worker has sent us a message with its current status, handle it.
     * 
     * @param m
     *            The update message.
     */
    protected abstract void handleNodeUpdateMessage(UpdateNodeMessage m);

    /**
     * A worker has sent use a completion message for a task. Process it.
     * 
     * @param result
     *            The message.
     */
    protected void handleTaskCompletedMessage(TaskCompletedMessage result) {
        if (Settings.traceNodeProgress) {
            Globals.log
                    .reportProgress("Received a worker task completed message "
                            + result);
        }
        final TaskInstance task = nodes.registerTaskCompleted(result);
        if (task != null) {
            masterQueue.removeDuplicates(task);
        }
    }

    /**
     * A worker has sent use a received message for a task. Process it.
     * 
     * @param result
     *            The message.
     */
    private void handleTaskReceivedMessage(TaskReceivedMessage result) {
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Received a task received message "
                    + result);
        }
        nodes.registerTaskReceived(result);
    }

    /**
     * A worker has sent use a completion message for a task. Process it.
     * 
     * @param msg
     *            The status message.
     */
    private void handleTaskFailMessage(TaskFailMessage msg) {
        if (Settings.traceNodeProgress) {
            Globals.log.reportProgress("Received a worker task failed message "
                    + msg);
        }
        final TaskInstance failedTask = nodes.registerTaskFailed(msg.source,
                msg.id);
        Globals.log.reportError("Node " + msg.source
                + " failed to execute task with id " + msg.id
                + "; node will no longer get tasks of this type");
        masterQueue.add(failedTask);
    }

    /**
     * A worker has sent use a completion message for a task. Process it.
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

    abstract void handleAntInfoMessage(AntInfoMessage antInfoMessage);

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

    protected abstract void registerNewGossipHasArrived();

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
        } else if (msg instanceof TaskCompletedMessage) {
            handleTaskCompletedMessage((TaskCompletedMessage) msg);
        } else if (msg instanceof TaskReceivedMessage) {
            handleTaskReceivedMessage((TaskReceivedMessage) msg);
        } else if (msg instanceof JobResultMessage) {
            handleJobResultMessage((JobResultMessage) msg);
        } else if (msg instanceof RunTaskMessage) {
            handleRunTaskMessage((RunTaskMessage) msg);
        } else if (msg instanceof TaskFailMessage) {
            handleTaskFailMessage((TaskFailMessage) msg);
        } else if (msg instanceof StopNodeMessage) {
            handleStopNodeMessage((StopNodeMessage) msg);
        } else if (msg instanceof AntInfoMessage) {
            handleAntInfoMessage((AntInfoMessage) msg);
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
    }

    private void restartLateJobs() {
        if (masterQueue.isEmpty() && workerQueue.isEmpty() && !stopped.isSet()) {
            final TaskInstance job = runningJobs.getLateJob();
            if (job != null) {
                Globals.log.reportProgress("Resubmitted late job " + job);
                masterQueue.add(job);
            }
        }

    }

    /** On a locked queue, try to send out as many task as we can. */
    protected abstract void drainMasterQueue();

    void submit(TaskInstance task) {
        masterQueue.add(task);
    }

    /**
     * Given an input and a list of possible jobs to execute, submit this input
     * as a job with the best promised completion time. If
     * <code>submitIfBusy</code> is set, also consider jobs where all workers
     * are currently busy.
     * 
     * @param input
     *            The input of the job.
     * @param userId
     *            The identifier associated with this job instance.
     * @param submitIfBusy
     *            If set, also consider jobs for which all workers are currently
     *            busy.
     * @param listener
     *            The completion listener for this job.
     * @param choices
     *            The list of job choices.
     * @return <code>true</code> if the job could be submitted.
     */
    public abstract boolean submit(Object input, Serializable userId,
            boolean submitIfBusy, JobCompletionListener listener,
            Job... choices);

    private boolean keepRunning() {
        if (!stopped.isSet()) {
            return true;
        }
        if (runningTasks.isAbove(0)) {
            return true;
        }
        return false;
    }

    abstract void handleTaskResult(RunTaskMessage message, Object result,
            long runMoment);

    private void executeTask(RunTaskMessage message, Task task, Object input,
            long runMoment) {
        if (task instanceof AtomicTask) {
            final AtomicTask at = (AtomicTask) task;
            try {
                final Object result = at.run(input, this);
                handleTaskResult(message, result, runMoment);
            } catch (final TaskFailedException x) {
                failNode(message, x);
            }
        } else if (task instanceof MapReduceTask) {
            final MapReduceTask mrt = (MapReduceTask) task;
            final MapReduceHandler handler = new MapReduceHandler(this, mrt,
                    message, runMoment);
            mrt.map(input, handler);
            handler.start();
        } else if (task instanceof AlternativesTask) {
            Globals.log
                    .reportInternalError("AlternativesTask should have been selected by the master "
                            + task);
        } else {
            Globals.log
                    .reportInternalError("Don't know what to do with a task of type "
                            + task.getClass());
        }
    }

    protected abstract RunTaskMessage getWork();

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
        long overheadStart = System.nanoTime();
        long threadOverhead = 0L;
        try {
            while (keepRunning()) {
                RunTaskMessage message = null;
                boolean readyForWork = false;

                updateAdministration();
                if (runningTasks.isBelow(numberOfProcessors)) {
                    // Only try to start a new task when there are idle
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
                                        + "ms for new tasks in queue");
                            }
                        }
                        synchronized (this) {
                            final long overhead = System.nanoTime()
                                    - overheadStart;
                            // Measure the time we spend in this wait,
                            // and add to idle time.
                            if (keepRunning()) {
                                this.wait(sleepTime);
                            }
                            overheadStart = System.nanoTime();
                            threadOverhead += overhead;
                        }
                    } catch (final InterruptedException e) {
                        // Not interesting.
                    }
                    idleProcessors.down();
                } else {
                    // We have a task to execute.
                    final long runMoment = System.nanoTime();
                    final TaskType type = message.taskInstance.type;
                    final Task task = jobs.getTask(type);

                    runningTasks.up();
                    if (Settings.traceNodeProgress) {
                        final long queueInterval = runMoment
                                - message.arrivalMoment;
                        Globals.log.reportProgress("Worker: handed out task "
                                + message + " of type " + type
                                + "; it was queued for "
                                + Utils.formatNanoseconds(queueInterval)
                                + "; there are now " + runningTasks
                                + " running tasks");
                    }
                    final Object input = message.taskInstance.input;
                    threadOverhead += System.nanoTime() - overheadStart;
                    executeTask(message, task, input, runMoment);
                    overheadStart = System.nanoTime();
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

    protected void failNode(RunTaskMessage message, Throwable t) {
        final TaskType type = message.taskInstance.type;
        Globals.log.reportError("Node fails for type " + type);
        t.printStackTrace(Globals.log.getPrintStream());
        final boolean allFailed = workerQueue.failTask(type);
        sendTaskFailMessage(message.source, message.taskId);
        if (allFailed && !isMaestro) {
            setStopped();
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
    public abstract int waitForReadyNodes(int n, long maximalWaitTime);

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
        if (Settings.USE_ANT_ROUTING) {
            return new AntRoutingNode(jobs, goForMaestro);
        }
        return new QRoutingNode(jobs, goForMaestro);
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
}
