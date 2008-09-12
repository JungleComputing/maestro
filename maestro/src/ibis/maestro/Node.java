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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 * A node in the Maestro dataflow network.
 * 
 * @author Kees van Reeuwijk
 *
 */
public final class Node extends Thread implements PacketReceiveListener
{
    static final IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.MEMBERSHIP_UNRELIABLE, IbisCapabilities.ELECTIONS_STRICT );
    final PacketSendPort sendPort;
    final PacketUpcallReceivePort receivePort;
    final long startTime;
    private long stopTime = 0;
    private static final String MAESTRO_ELECTION_NAME = "maestro-election";

    private RegistryEventHandler registryEventHandler;

    private final RecentMasterList recentMasterList = new RecentMasterList();

    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    private static final int workThreadCount = numberOfProcessors+Settings.EXTRA_WORK_THREADS;
    private final WorkThread workThreads[] = new WorkThread[workThreadCount];
    private final Gossiper gossiper;
    private final Terminator terminator;

    private CompletedJobJist completedJobList = new CompletedJobJist();

    private IbisIdentifier maestro = null;

    /** The list of running jobs with their completion listeners. */
    private final RunningJobs runningJobs = new RunningJobs();

    /** The list of nodes we know about. */
    private final NodeList nodes;

    private boolean isMaestro;

    private final boolean traceStats;
    private final MasterQueue masterQueue;
    private final WorkerQueue workerQueue;
    private long nextTaskId = 0;
    private final Flag doUpdateRecentMasters = new Flag( false );
    private final Flag recomputeCompletionTimes = new Flag( false );
    private UpDownCounter idleProcessors = new UpDownCounter( -Settings.EXTRA_WORK_THREADS ); // Yes, we start with a negative number of idle processors.
    private Counter updateMessageCount = new Counter();
    private Counter submitMessageCount = new Counter();
    private Counter taskResultMessageCount = new Counter();
    private Counter jobResultMessageCount = new Counter();
    private Counter taskFailMessageCount = new Counter();
    private Flag enableRegistration = new Flag( false );

    private Flag stopped = new Flag( false );

    private UpDownCounter runningTasks = new UpDownCounter( 0 );
    private final JobList jobs;

    private final class NodeRegistryEventHandler implements RegistryEventHandler {
        /**
         * A new Ibis joined the computation.
         * @param theIbis The ibis that joined the computation.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public void joined( IbisIdentifier theIbis )
        {
            boolean local = theIbis.equals( Globals.localIbis.identifier() );
            sendPort.registerDestination( theIbis );
            if( !local ){
                gossiper.registerNode( theIbis );
                if( terminator != null ) {
                    terminator.registerNode( theIbis );
                }
            }
            nodes.registerNode( theIbis, local );
        }

        /**
         * An ibis has died.
         * @param theIbis The ibis that died.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public void died( IbisIdentifier theIbis )
        {
            Globals.log.reportProgress( "Ibis " + theIbis + " has died" );
            registerIbisLeft( theIbis );
        }

        /**
         * An ibis has explicitly left the computation.
         * @param theIbis The ibis that left.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public void left( IbisIdentifier theIbis )
        {
            registerIbisLeft( theIbis );
        }

        /**
         * The results of an election are known.
         * @param name The name of the election.
         * @param theIbis The ibis that was elected.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public void electionResult( String name, IbisIdentifier theIbis )
        {
            if( name.equals( MAESTRO_ELECTION_NAME ) && theIbis != null ){
                Globals.log.reportProgress( "Ibis " + theIbis + " was elected maestro" );
                maestro = theIbis;
            }
        }

        /**
         * Our ibis got a signal.
         * @param signal The signal.
         */
        @Override
        public void gotSignal( String signal )
        {
            // Not interested.
        }
    }

    /**
     * Constructs a new Maestro node using the given list of jobs. Optionally
     * try to get elected as maestro.
     * @param jobs The jobs that should be supported in this node.
     * @param runForMaestro If true, try to get elected as maestro.
     * @throws IbisCreationFailedException Thrown if for some reason we cannot create an ibis.
     * @throws IOException Thrown if for some reason we cannot communicate.
     */
    @SuppressWarnings("synthetic-access")
    public Node( JobList jobs, boolean runForMaestro ) throws IbisCreationFailedException, IOException
    {
        Properties ibisProperties = new Properties();

        this.jobs = jobs;
        TaskType supportedTypes[] = jobs.getSupportedTaskTypes();
        TaskType[] allTypes = jobs.getAllTypes();
        Globals.allTaskTypes = allTypes;
        Globals.supportedTaskTypes = supportedTypes;
        masterQueue = new MasterQueue( allTypes );
        workerQueue = new WorkerQueue( jobs );
        nodes = new NodeList( workerQueue );
        ibisProperties.setProperty( "ibis.pool.name", "MaestroPool" );
        registryEventHandler = new NodeRegistryEventHandler();
        Globals.localIbis = IbisFactory.createIbis(
            ibisCapabilities,
            ibisProperties,
            true,
            registryEventHandler,
            PacketSendPort.portType,
            PacketUpcallReceivePort.portType
        );
        Ibis localIbis = Globals.localIbis;
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Created ibis " + localIbis );
        }
        nodes.registerNode( localIbis.identifier(), true );
        Registry registry = localIbis.registry();
        if( runForMaestro ){
            IbisIdentifier m = registry.elect( MAESTRO_ELECTION_NAME );
            isMaestro = m.equals( localIbis.identifier() );
            if( isMaestro ) {
                enableRegistration.set();   // We're maestro, we're allowed to register with others.
            }
        }
        else {
            isMaestro = false;

        }
        Globals.log.reportProgress( "Started ibis " + localIbis.identifier() + ": isMaestro=" + isMaestro );
        if( !isMaestro && Globals.supportedTaskTypes.length == 0 ) {
            Globals.log.reportProgress( "This node does not support any types, and isn't the maestro. Stopping" );
            stopped.set();
        }
        sendPort = new PacketSendPort( this, localIbis.identifier() );
        gossiper = new Gossiper( sendPort, isMaestro, jobs );
        gossiper.start();
        terminator = buildTerminator();
        receivePort = new PacketUpcallReceivePort( localIbis, Globals.receivePortName, this );
        for( int i=0; i<workThreads.length; i++ ) {
            WorkThread t = new WorkThread( this );
            workThreads[i] = t;
            t.start();
        }
        this.traceStats = System.getProperty( "ibis.maestro.traceWorkerStatistics" ) != null;
        startTime = System.nanoTime();
        start();
        registry.enableEvents();
        if( Settings.traceNodes ) {
            Globals.log.log( "Started a Maestro node" );
        }
    }
    
    private Terminator buildTerminator()
    {
	if( !isMaestro ) {
	    // We only run a terminator on the maestro.
	    return null;
	}
	String terminatorStartQuotumString = System.getProperty( "ibis.maestro.terminatorStartQuotum" );
	String terminatorNodeQuotumString = System.getProperty( "ibis.maestro.terminatorNodeQuotum" );
	String terminatorInitialSleepString = System.getProperty( "ibis.maestro.terminatorInitialSleepTime" );
	String terminatorSleepString = System.getProperty( "ibis.maestro.terminatorSleepTime" );
	try {
	    if( terminatorInitialSleepString == null ) {
	        return null;
	    }
            long initialSleep = Long.parseLong( terminatorInitialSleepString );
	    double startQuotum;
            double nodeQuotum;
            long sleep;
            if( terminatorStartQuotumString == null ) {
                startQuotum = Settings.DEFAULT_TERMINATOR_START_QUOTUM;
            }
            else {
                startQuotum = Double.parseDouble(terminatorStartQuotumString );
            }
            if( terminatorNodeQuotumString == null ) {
                nodeQuotum = Settings.DEFAULT_TERMINATOR_NODE_QUOTUM;
            }
            else {
                nodeQuotum = Double.parseDouble(terminatorNodeQuotumString );
            }
            if( terminatorSleepString == null ) {
                sleep = Settings.DEFAULT_TERMINATOR_SLEEP;
            }
            else {
                sleep = Long.parseLong( terminatorSleepString );
            }
	    Terminator t = new Terminator( startQuotum, nodeQuotum, initialSleep, sleep );
            t.start();
            Globals.log.reportProgress( "Started terminator" );
	    return t;
	}
	catch( Throwable e ) {
	    Globals.log.reportInternalError( "Bad terminator specification: " + e.getLocalizedMessage() );
	}
	return null;
    }

    /**
     * Start this thread.
     * Do not invoke this method, it is already invoked in the
     * constructor of this node.
     */
    @Override
    public void start()
    {
        receivePort.enable();           // We're open for business.
        super.start();                  // Start the thread
    }

    /** Set this node to the stopped state.
     * This does not mean that the node stops immediately,
     * but it does mean the master and worker try to wind down the work.
     * @param clearQueues If <code>true</code> clear all work queues.
     */
    public void setStopped( boolean clearQueues )
    {
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Set node to stopped state" );
        }
        stopped.set();
        if( clearQueues ) {
            masterQueue.clear();
            workerQueue.clear();
        }
        kickAllWorkers();
    }

    /** Set this node to the stopped state.
     * This does not mean that the node stops immediately,
     * but it does mean the master and worker try to wind down the work.
     */
    public void setStopped()
    {
        setStopped( false );
    }

    /**
     * Wait for this node to finish.
     */
    public void waitToTerminate()
    {
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Waiting for node to terminate" );
        }

        waitForWorkThreadsToTerminate();
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Node has terminated" );
        }
        try {
            Globals.localIbis.end();
        }
        catch( IOException x ) {
            // Nothing we can do about it.
        }
        printStatistics( Globals.log.getPrintStream() );
    }

    /**
     * Returns true iff this node is a maestro.
     * @return True iff this node is a maestro.
     */
    public boolean isMaestro() { return isMaestro; }

    private synchronized void kickAllWorkers()
    {
        this.notifyAll();
    }

    /** Registers the ibis with the given identifier as one that has left the
     * computation.
     * @param theIbis The ibis that has left.
     */
    private void registerIbisLeft( IbisIdentifier theIbis )
    {
        gossiper.removeNode( theIbis );
        if( terminator != null ) {
            terminator.removeNode( theIbis );
        }
        recentMasterList.remove( theIbis );
        ArrayList<TaskInstance> orphans = nodes.removeNode( theIbis );
        for( TaskInstance ti: orphans ) {
            ti.setOrphan();
        }
        masterQueue.add( orphans );
        if( maestro != null && theIbis.equals( maestro ) ) {
            Globals.log.reportProgress( "The maestro has left; stopping.." );
            setStopped();
            kickAllWorkers();
        }
        else if( theIbis.equals( Globals.localIbis.identifier() ) ) {
            // The registry has declared us dead. We might as well stop.
            Globals.log.reportProgress( "This node has been declared dead, stopping.." );
            setStopped();
            masterQueue.clear();    // Nobody seems to be interested in any work we still have.
            kickAllWorkers();
        }
    }

    /** Report the completion of the job with the given identifier.
     * @param id The job that has been completed.
     * @param result The job result.
     */
    private void reportCompletion( JobInstanceIdentifier id, Object result )
    {
        JobInstanceInfo job = runningJobs.remove( id );
        if( job != null ){
            job.listener.jobCompleted( this, id.userId, result );
        }
    }

    void addRunningJob( JobInstanceIdentifier id, Job job, JobCompletionListener listener )
    {
        runningJobs.add( new JobInstanceInfo( id, job, listener ) );
    }

    /** This ibis was reported as 'may be dead'. Try
     * not to communicate with it.
     * @param theIbis The ibis that may be dead.
     */
    void setSuspect( IbisIdentifier theIbis )
    {
        try {
            Globals.localIbis.registry().assumeDead( theIbis );
        }
        catch( IOException e )
        {
            // Nothing we can do about it.
        }
        nodes.setSuspect( theIbis );
    }

    private void drainCompletedJobList()
    {
        while( true ) {
            CompletedJob j = completedJobList.removeIfAny();
            if( j == null ) {
                break;
            }
            reportCompletion( j.job, j.result );
        }
    }

    /**
     * Do all updates of the node adminstration that we can.
     * 
     */
    private void updateAdministration()
    {
        if( recomputeCompletionTimes.getAndReset() ){
            long masterQueueIntervals[] = masterQueue.getQueueIntervals();
            HashMap<IbisIdentifier,LocalNodeInfo> localNodeInfoMap = nodes.getLocalNodeInfo();
            gossiper.recomputeCompletionTimes( masterQueueIntervals, jobs, localNodeInfoMap );
        }
        if( doUpdateRecentMasters.getAndReset() ) {
            updateRecentMasters();
        }
        drainCompletedJobList();
        drainMasterQueue();
        nodes.checkDeadlines( System.nanoTime() );
    }

    /** Print some statistics about the entire worker run. */
    synchronized void printStatistics( PrintStream s )
    {
        if( stopTime<startTime ) {
            Globals.log.reportError( "Node didn't stop yet" );
            stopTime = System.nanoTime();
        }
        s.printf(  "# work threads  = %5d\n", workThreads.length );
        nodes.printStatistics( s );
        jobs.printStatistics( s );
        s.printf( "update       messages:   %5d sent\n", updateMessageCount.get() );
        s.printf( "submit       messages:   %5d sent\n", submitMessageCount.get() );
        s.printf( "task result  messages:   %5d sent\n", taskResultMessageCount.get() );
        s.printf( "job result   messages:   %5d sent\n", jobResultMessageCount.get() );
        s.printf( "job fail     messages:   %5d sent\n", taskFailMessageCount.get() );
        gossiper.printStatistics( s );
        if( terminator != null ) {
            terminator.printStatistics( s );
        }
        sendPort.printStatistics( s, "send port" );
        long activeTime = workerQueue.getActiveTime( startTime );
        long workInterval = stopTime-activeTime;
        workerQueue.printStatistics( s, workInterval );
        s.println( "run time        = " + Utils.formatNanoseconds( workInterval ) );
        s.println( "activated after = " + Utils.formatNanoseconds( activeTime-startTime ) );
        masterQueue.printStatistics( s );
        Utils.printThreadStats( s );    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * @param id The job instance identifier.
     * @param result The result to send.
     * @return <code>true</code> if the message could be sent.
     */
    private boolean sendJobResultMessage( JobInstanceIdentifier id, Object result )
    {
        Message msg = new JobResultMessage( id, result );	
        jobResultMessageCount.add();
        return sendPort.send( id.ibis, msg );
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * @param taskId The task instance that failed.
     * @return <code>true</code> if the message could be sent.
     */
    private boolean sendTaskFailMessage( IbisIdentifier ibis, long taskId )
    {
        NodePerformanceInfo update = gossiper.getLocalUpdate();
        Message msg = new TaskFailMessage( taskId, update );
        taskFailMessageCount.add();
        return sendPort.send( ibis, msg );
    }

    private void updateRecentMasters()
    {
        NodePerformanceInfo update = gossiper.getLocalUpdate();
        UpdateNodeMessage msg = new UpdateNodeMessage( update );
        for( IbisIdentifier ibis: recentMasterList.getArray() ){	    
            if( Settings.traceUpdateMessages ) {
                Globals.log.reportProgress( "Sending " + msg + " to " + ibis );
            }
            sendPort.send( ibis, msg );
            updateMessageCount.add();
        }
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * @param m The update message.
     * @param nw <code>true</code> iff this update was new.
     */
    private boolean handleNodeUpdateInfo( NodePerformanceInfo m )
    {
        if( Settings.traceNodeProgress ){
            Globals.log.reportProgress( "Received node update message " + m );
        }
        NodeInfo nodeInfo = nodes.get( m.source );   // The get will create an entry if necessary.
        boolean changed = false;
        changed = nodeInfo.registerWorkerQueueInfo( m.workerQueueInfo );
        changed |= nodeInfo.registerAsCommunicating();
        return changed;
    }

    /**
     * A node has sent us an accept message, handle it.
     * @param m The update message.
     */
    private void handleGossipMessage( GossipMessage m )
    {
	if( Settings.traceNodeProgress || Settings.traceRegistration || Settings.traceGossip ){
	    Globals.log.reportProgress( "Received gossip message from " + m.source + " with " + m.gossip.length + " items"  );
	}
	boolean changed = false;
	for( NodePerformanceInfo i: m.gossip ) {
	    boolean changed1 = gossiper.registerGossip( i, m.source );
	    changed |= handleNodeUpdateInfo( i );
	}
	if( m.needsReply ) {
	    if( !m.source.equals( Globals.localIbis.identifier() ) ) {
		gossiper.queueGossipReply( m.source );
	    }
	}
	if( changed ) {
	    synchronized( this ) {
		this.notify();
	    }
	}
    }

    /**
     * Handle a message containing a new task to run.
     * 
     * @param msg The message to handle.
     */
    private void handleRunTaskMessage( RunTaskMessage msg )
    {
        IbisIdentifier source = msg.source;
        boolean isDead = nodes.registerAsCommunicating( source );
        if( !isDead ) {
            recentMasterList.register( source );
        }
        doUpdateRecentMasters.set();
        workerQueue.add( msg );
        gossiper.incrementLocalQueueLength( msg.taskInstance.type );
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * @param m The update message.
     */
    private void handleNodeUpdateMessage( UpdateNodeMessage m )
    {
        if( Settings.traceNodeProgress ){
            Globals.log.reportProgress( "Received node update message " + m );
        }
        boolean isnew = gossiper.registerGossip( m.update, m.update.source );
        isnew |= handleNodeUpdateInfo( m.update );
        if( isnew ) {
            recomputeCompletionTimes.set();
        }
    }

    /**
     * A worker has sent use a completion message for a task. Process it.
     * @param result The status message.
     */
    private void handleTaskCompletedMessage( TaskCompletedMessage result )
    {
        if( Settings.traceNodeProgress ){
            Globals.log.reportProgress( "Received a worker task completed message " + result );
        }
        TaskInstance task = nodes.registerTaskCompleted( result );
        if( task != null ) {
            masterQueue.removeDuplicates( task );
        }
        doUpdateRecentMasters.set();
    }

    /**
     * A worker has sent use a completion message for a task. Process it.
     * @param msg The status message.
     */
    private void handleTaskFailMessage( TaskFailMessage msg )
    {
        if( Settings.traceNodeProgress ){
            Globals.log.reportProgress( "Received a worker task failed message " + msg );
        }
        TaskInstance failedTask = nodes.registerTaskFailed( msg.source, msg.id );
        Globals.log.reportError( "Node " + msg.source + " failed to execute task with id " + msg.id + "; node will no longer get tasks of this type" );
        boolean isnew = gossiper.registerGossip( msg.update, msg.update.source );
        isnew |= handleNodeUpdateInfo( msg.update );
        if( isnew ) {
            recomputeCompletionTimes.set();
        }
        masterQueue.add( failedTask );
    }

    /**
     * A worker has sent use a completion message for a task. Process it.
     * @param msg The status message.
     */
    private void handleStopNodeMessage( StopNodeMessage msg )
    {
	Globals.log.reportProgress( "Node was forced to stop by " + msg.source );
	System.exit( 2 );
    }

    private void handleJobResultMessage( JobResultMessage m )
    {
        completedJobList.add( new CompletedJob( m.job, m.result ) );
    }

    /**
     * Handles message <code>msg</code> from worker.
     * @param msg The message we received.
     */
    @Override
    public void messageReceived( Message msg )
    {
        handleMessage( msg );
    }

    /** Handle the given message. */
    void handleMessage( Message msg )
    {
        if( Settings.traceNodeProgress ){
            Globals.log.reportProgress( "Received message " + msg );
        }
        if( msg instanceof UpdateNodeMessage ) {
            handleNodeUpdateMessage( (UpdateNodeMessage) msg );
        }
        else if( msg instanceof GossipMessage ) {
            handleGossipMessage( (GossipMessage) msg );
        }
        else if( msg instanceof TaskCompletedMessage ) {
            handleTaskCompletedMessage( (TaskCompletedMessage) msg );
        }
        else if( msg instanceof JobResultMessage ) {
            handleJobResultMessage( (JobResultMessage) msg );
        }
        else if( msg instanceof RunTaskMessage ){
            handleRunTaskMessage( (RunTaskMessage) msg );
        }
        else if( msg instanceof TaskFailMessage ) {
            handleTaskFailMessage( (TaskFailMessage) msg );
        }
        else if( msg instanceof StopNodeMessage ) {
            handleStopNodeMessage( (StopNodeMessage) msg );
        }
        else {
            Globals.log.reportInternalError( "the node should handle message of type " + msg.getClass() );
        }
        synchronized( this ) {
            this.notify();
        }
    }

    private void waitForWorkThreadsToTerminate()
    {
        for( WorkThread t: workThreads ) {
            if( Settings.traceNodes ){
                Globals.log.reportProgress( "Waiting for termination of thread " + t );
            }
            Utils.waitToTerminate( t );
        }
        gossiper.setStopped();
        synchronized( this ) {
            stopTime = System.nanoTime();
        }
    }
    
    /**
     * This object only exists to lock the critical section in drainMasterQueue,
     * and prevent that two threads select the same next task to submit to a worker. 
     */
    private final Flag drainLock = new Flag( false );

    /** On a locked queue, try to send out as many task as we can. */
    private void drainMasterQueue()
    {
	boolean changed = false;

	if( masterQueue.isEmpty() ) {
            // Nothing to do, don't bother with the gossip.
            return;
        }
        while( true ) {
            NodeInfo worker;
            long taskId;
            IbisIdentifier node;
            TaskInstance task;

            synchronized( drainLock ) {
                NodePerformanceInfo[] tables = gossiper.getGossipCopy();
                HashMap<IbisIdentifier,LocalNodeInfo> localNodeInfoMap = nodes.getLocalNodeInfo();
                Submission submission = masterQueue.getSubmission( localNodeInfoMap, tables );
                if( submission == null ) {
                    break;
                }
                node = submission.worker;
                task = submission.task;
                worker = nodes.get( node );
                taskId = nextTaskId++;

                worker.registerTaskStart( task, taskId, submission.predictedDuration );
            }
            if( Settings.traceMasterQueue || Settings.traceSubmissions ) {
                Globals.log.reportProgress( "Submitting task " + task + " to " + node );
            }
            RunTaskMessage msg = new RunTaskMessage( node, task, taskId );
            boolean ok = sendPort.send( node, msg );
            if( ok ){
                submitMessageCount.add();
            }
            else {
                // Try to put the paste back in the tube.
                // The send port has already registered the trouble.
                masterQueue.add( msg.taskInstance );
                worker.retractTask( taskId );
            }
            changed = true;
        }
        if( changed ) {
            recomputeCompletionTimes.set();
        }
    }

    /**
     * Adds the given task to the work queue of this master.
     * @param task The task instance to add to the queue.
     */
    void submit( TaskInstance task )
    {
        if( Settings.traceNodeProgress || Settings.traceMasterQueue ) {
            Globals.log.reportProgress( "Master: received task " + task );
        }
        masterQueue.add( task );
        synchronized( this ) {
            this.notify();   // Wake a thread to handle this.
        }
    }

    /**
     * Given an input and a list of possible jobs to execute, submit
     * this input as a job with the best promised completion time.
     * If <code>submitIfBusy</code> is set, also consider jobs where all
     * workers are currently busy.
     * @param input The input of the job.
     * @param submitIfBusy If set, also consider jobs for which all workers are currently busy.
     * @param listener The completion listener for this job.
     * @param choices The list of job choices.
     * @return <code>true</code> if the job could be submitted.
     */
    boolean submit( Object input, boolean submitIfBusy, JobCompletionListener listener, Job...choices )
    {
        int choice;

        if( choices.length == 0 ){
            // No choices? Obviously we won't be able to submit this one.
            return false;
        }
        if( choices.length == 1 && submitIfBusy ){
            choice = 0;
        }
        else {
            TaskType types[] = new TaskType[choices.length];

            for( int ix=0; ix<choices.length; ix++ ) {
                Job job = choices[ix];

                types[ix] = job.getFirstTaskType();
            }
            HashMap<IbisIdentifier,LocalNodeInfo> localNodeInfoMap = nodes.getLocalNodeInfo();
            choice = gossiper.selectFastestTask( types, submitIfBusy, localNodeInfoMap );
            if( choice<0 ) {
                // Couldn't submit the job.
                return false;
            }
        }
        Job job = choices[choice];
        job.submit( this, input, job, listener );
        return true;
    }

    private boolean keepRunning()
    {
        if( !stopped.isSet() ) {
            return true;
        }
        if( runningTasks.isAbove( 0 ) ) {
            return true;
        }
        return false;
    }

    private void executeTask( RunTaskMessage message, Task task, Object input, long runMoment )
    {
        if( task instanceof AtomicTask ) {
            AtomicTask at = (AtomicTask) task;
            try {
                Object result = at.run( input, this );
                handleTaskResult( message, result, runMoment );
            }
            catch( TaskFailedException x ) {
                failNode( message, x );
            }
        }
        else if( task instanceof MapReduceTask ) {
            MapReduceTask mrt = (MapReduceTask) task;
            MapReduceHandler handler = new MapReduceHandler( this, mrt, message, runMoment );
            mrt.map( input, handler );
            handler.start();
        }
        else if( task instanceof AlternativesTask ) {
            Globals.log.reportInternalError( "AlternativesTask should have been selected by the master " + task );
        }
        else {
            Globals.log.reportInternalError( "Don't know what to do with a task of type " + task.getClass() );
        }
    }

    /** Run a work thread. Only return when we want to shut down the node. */
    void runWorkThread()
    {
        try {
            while( keepRunning() ) {
                RunTaskMessage message = null;

                updateAdministration();
                if( runningTasks.isBelow( numberOfProcessors ) ) {
                    // Only try to start a new task when there are idle
                    // processors.
                    message = workerQueue.remove();
                }
                if( message == null ) {
                    idleProcessors.up();
                    long sleepTime = 100;
                    if( Settings.traceWaits ) {
                        Globals.log.reportProgress( "Waiting for " + sleepTime + "ms for new tasks in queue" );
                    }
                    // Wait a little, there is nothing to do.
                    try{
                        synchronized( this ) {
                            if( keepRunning() ) {
                                this.wait( sleepTime );
                            }
                        }
                    }
                    catch( InterruptedException e ){
                        // Not interesting.
                    }
                    idleProcessors.down();
                }
                else {
                    // We have a task to execute.
                    long runMoment = System.nanoTime();
                    TaskType type = message.taskInstance.type;
                    long queueInterval = runMoment-message.getQueueMoment();
                    int queueLength = message.getQueueLength();
                    Task task = jobs.getTask( type );

                    workerQueue.setQueueTimePerTask( type, queueInterval, queueLength );
                    gossiper.setQueueTimePerTask( type, queueInterval );

                    runningTasks.up();
                    if( Settings.traceNodeProgress ) {
                        Globals.log.reportProgress( "Worker: handed out task " + message + " of type " + type + "; it was queued for " + Utils.formatNanoseconds( queueInterval ) + "; there are now " + runningTasks + " running tasks" );
                    }
                    Object input = message.taskInstance.input;
                    executeTask( message, task, input, runMoment );
                    if( Settings.traceNodeProgress ) {
                        Globals.log.reportProgress( "Work thread: completed " + message );
                    }
                }
            }
        }
        catch( Throwable x ) {
            Globals.log.reportError( "Uncaught exception in worker thread: " + x.getLocalizedMessage() );
            x.printStackTrace( Globals.log.getPrintStream() );
        }
        kickAllWorkers(); // We're about to end this thread. Wake all other threads.
    }

    private void failNode( RunTaskMessage message, Throwable t )
    {
        TaskType type = message.taskInstance.type;
        Globals.log.reportError( "Node fails for type " + type );
        t.printStackTrace( Globals.log.getPrintStream() );
        boolean allFailed = workerQueue.failTask( type );
        gossiper.failTask( type );
        sendTaskFailMessage( message.source, message.taskId );
        if( allFailed && !isMaestro ) {
            setStopped();
        }
            
    }

    /**
     * @param message
     * @param result
     * @param runMoment
     */
    void handleTaskResult( RunTaskMessage message, Object result, long runMoment )
    {
        long taskCompletionMoment = System.nanoTime();

        TaskType type = message.taskInstance.type;
        taskResultMessageCount.add();

        TaskType nextTaskType = jobs.getNextTaskType( type );
        if( nextTaskType == null ){
            // This was the final step. Report back the result.
            JobInstanceIdentifier identifier = message.taskInstance.jobInstance;
            boolean ok = sendJobResultMessage( identifier, result );
            if( !ok ) {
        	// Could not send the result message. We're in trouble.
        	// Just try again.
        	ok = sendJobResultMessage(identifier, result );
        	if( !ok ) {
        	    // Nothing we can do, we give up.
        	    Globals.log.reportError( "Could not send job result message to " + identifier );
        	}
            }
        }
        else {
            // There is a next step to take.
            TaskInstance nextTask = new TaskInstance( message.taskInstance.jobInstance, nextTaskType, result );
            submit( nextTask );
        }

        // Update statistics.
        final long computeInterval = taskCompletionMoment-runMoment;
        long averageComputeTime = workerQueue.countTask( type, computeInterval );
        gossiper.setComputeTime( type, averageComputeTime );
        runningTasks.down();
        if( Settings.traceNodeProgress || Settings.traceRemainingJobTime ) {
            long queueInterval = runMoment-message.getQueueMoment();
            Globals.log.reportProgress( "Completed " + message.taskInstance + "; queueInterval=" + Utils.formatNanoseconds( queueInterval ) + "; runningTasks=" + runningTasks );
        }
        long workerDwellTime = taskCompletionMoment-message.getQueueMoment();
        if( traceStats ) {
            double now = 1e-9*(System.nanoTime()-startTime);
            System.out.println( "TRACE:workerDwellTime " + type + " " + now + " " + 1e-9*workerDwellTime );
        }
        Message msg = new TaskCompletedMessage( message.taskId, workerDwellTime );
        boolean ok = sendPort.send( message.source, msg );

        if( !ok ) {
            // Could not send the result message. We're desperate.
            // First simply try again.
            ok = sendPort.send( message.source, msg );
            if( !ok ) {
        	// Unfortunately, that didn't work.
        	// TODO: think up another way to recover from failed result report.
        	Globals.log.reportError( "Failed to send task completed message to " + message.source );
            }
        }
        doUpdateRecentMasters.set();
    }

    /**
     * Given a number of nodes to wait for, keep waiting until we have gossip information about
     * at least this many nodes, or until the given time has elapsed.
     * @param n The number of nodes to wait for.
     * @param maximalWaitTime The maximal time in ms to wait for these nodes.
     * @return The actual number of nodes there was information for at the moment we stopped waiting.
     */
    public int waitForReadyNodes( int n, long maximalWaitTime )
    {
        return gossiper.waitForReadyNodes( n, maximalWaitTime );
    }

    /**
     * Writes the given progress message to the logger.
     * @param msg The message to write.
     */
    public void reportProgress( String msg )
    {
        Globals.log.reportProgress( msg );
    }

    /**
     * Writes the given error message to the logger.
     * @param msg The message to write.
     */
    public void reportError( String msg )
    {
        Globals.log.reportError( msg );
    }

    /**
     * Writes the given error message about an internal inconsistency in the program to the logger.
     * @param msg The message to write.
     */
    public void reportInternalError( String msg )
    {
        Globals.log.reportInternalError( msg );
    }
}
