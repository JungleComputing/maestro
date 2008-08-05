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
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.Semaphore;

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

    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    private static final int workThreadCount = numberOfProcessors+2;
    private final WorkThread workThreads[] = new WorkThread[workThreadCount];
    private final Gossiper gossiper;

    private final TaskSources taskSources = new TaskSources();

    /** The sender of non-essential messages. */
    private final NonEssentialSender nonEssentialSender;

    private CompletedJobJist completedJobList = new CompletedJobJist();

    private IbisIdentifier maestro = null;

    /** The list of running jobs with their completion listeners. */
    private final RunningJobs runningJobs = new RunningJobs();

    /** Info about all tasks. */
    private final TaskInfoList taskInfoList = new TaskInfoList();

    /** The list of nodes we know about. */
    private final NodeList nodes = new NodeList( taskInfoList );

    private boolean isMaestro;

    private final boolean traceStats;
    private final MasterQueue masterQueue;
    private final WorkerQueue workerQueue;
    private long nextTaskId = 0;
    private Counter handledTaskCount = new Counter();
    Counter registrationMessageCount = new Counter();
    private Counter acceptMessageCount = new Counter();
    private Counter updateMessageCount = new Counter();
    private Counter submitMessageCount = new Counter();
    private Counter taskResultMessageCount = new Counter();
    private Counter jobResultMessageCount = new Counter();
    private Flag enableRegistration = new Flag( false );

    private Flag stopped = new Flag( false );

    private UpDownCounter runningTasks = new UpDownCounter( 0 );
    private final JobList jobs;
    private Semaphore drainingQueue = new Semaphore( 1, false );

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
            sendPort.registerDestination( theIbis, local );
            gossiper.addNode( theIbis );
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
                System.out.println( "Ibis " + theIbis + " was elected maestro" );
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
     * Constructs a new Maestro node using the given name server and completion listener.
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
        TaskType taskTypes[] = jobs.getSupportedTaskTypes();
        masterQueue = new MasterQueue( taskTypes );
        workerQueue = new WorkerQueue( taskTypes );
        taskInfoList.registerLocalTasks( taskTypes, jobs );
        nonEssentialSender = new NonEssentialSender();
        nonEssentialSender.start();
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
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Ibis " + localIbis.identifier() + ": isMaestro=" + isMaestro );
        }
        gossiper = new Gossiper( this, isMaestro );
        gossiper.start();
        for( int i=0; i<workThreads.length; i++ ) {
            WorkThread t = new WorkThread( this );
            workThreads[i] = t;
            t.start();
        }
        receivePort = new PacketUpcallReceivePort( localIbis, Globals.receivePortName, this );
        sendPort = new PacketSendPort( this );
        sendPort.setLocalListener( this );    // FIXME: no longer necessary
        this.traceStats = System.getProperty( "ibis.maestro.traceWorkerStatistics" ) != null;
        startTime = System.nanoTime();
        start();
        registry.enableEvents();
        if( Settings.traceNodes ) {
            Globals.log.log( "Started a Maestro node" );
        }
    }

    /**
     * Start this thread.
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
     */
    public void setStopped()
    {
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Set node to stopped state" );
        }
        stopped.set();
        synchronized( this ) {
            // That's worth telling everyone.
            this.notifyAll();
        }
    }

    private boolean isStopped()
    {
        return stopped.isSet();
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
        printStatistics( System.out );
    }

    /**
     * Returns true iff this node is a maestro.
     * @return True iff this node is a maestro.
     */
    public boolean isMaestro() { return isMaestro; }

    /** Registers the ibis with the given identifier as one that has left the
     * computation.
     * @param theIbis The ibis that has left.
     */
    private void registerIbisLeft( IbisIdentifier theIbis )
    {
        gossiper.removeNode( theIbis );
        nonEssentialSender.removeMessagesToIbis( theIbis );
        ArrayList<TaskInstance> orphans = nodes.removeNode( theIbis );
        masterQueue.add( orphans );
        if( maestro != null && theIbis.equals( maestro ) ) {
            Globals.log.reportProgress( "The maestro has left; stopping.." );
            setStopped();
        }
        else if( theIbis.equals( Globals.localIbis.identifier() ) ) {
            // The registry has declared us dead. We might as well stop.
            Globals.log.reportProgress( "This node has been declared dead, stopping.." );
            setStopped();
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

    void addRunningJob( JobInstanceIdentifier id, Job job, CompletionListener listener )
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
        drainCompletedJobList();
        drainMasterQueue();
        nodes.checkDeadlines( System.nanoTime() );
    }

    private int getIdleTasks()
    {
        return Math.max( numberOfProcessors-runningTasks.get(), 0 );
    }

    /** Print some statistics about the entire worker run. */
    synchronized void printStatistics( PrintStream s )
    {
        if( stopTime<startTime ) {
            System.err.println( "Node didn't stop yet" );
            stopTime = System.nanoTime();
        }
        s.printf(  "# threads       = %5d\n", workThreads.length );
        nodes.printStatistics( s );
        jobs.printStatistics( s );
        s.printf( "registration messages:   %5d sent\n", registrationMessageCount.get() );
        s.printf( "accept       messages:   %5d sent\n", acceptMessageCount.get() );
        s.printf( "update       messages:   %5d sent\n", updateMessageCount.get() );
        s.printf( "submit       messages:   %5d sent\n", submitMessageCount.get() );
        s.printf( "task result  messages:   %5d sent\n", taskResultMessageCount.get() );
        s.printf( "job result   messages:   %5d sent\n", jobResultMessageCount.get() );
        gossiper.printStatistics( s );
        nonEssentialSender.printStatistics( s );
        sendPort.printStatistics( s, "send port" );
        long activeTime = workerQueue.getActiveTime( startTime );
        long workInterval = stopTime-activeTime;
        workerQueue.printStatistics( s, workInterval );
        taskInfoList.printStatistics( s, workInterval );
        s.println( "Worker: run time        = " + Service.formatNanoseconds( workInterval ) );
        s.println( "Worker: activated after = " + Service.formatNanoseconds( activeTime-startTime ) );
        masterQueue.printStatistics( s );
        s.printf(  "Master: # handled tasks  = %5d\n", handledTaskCount.get() );
    }

    void sendNonEssential( NonEssentialMessage msg )
    {
        if( msg.destination.equals( Globals.localIbis.identifier() ) ) {
            // Don't go through all the bureaucracy for a local message.
            msg.arrivalMoment = msg.sendMoment = System.nanoTime();
            messageReceived( msg );
        }
        else {
            nonEssentialSender.submit( msg );
        }
    }

    private void postUpdateNodeMessage( IbisIdentifier node )
    {
        NodeUpdateInfo update = buildLocalUpdate();
        UpdateNodeMessage msg = new UpdateNodeMessage( update );

        if( Settings.traceUpdateMessages ) {
            Globals.log.reportProgress( "Sending " + msg );
        }
        sendPort.tryToSend( node, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        updateMessageCount.add();
        gossiper.registerGossip( update );
    }

    /** FIXME.
     * @return
     */
    private NodeUpdateInfo buildLocalUpdate()
    {
        CompletionInfo[] completionInfo = masterQueue.getCompletionInfo( jobs, nodes, getIdleTasks() );
        WorkerQueueInfo[] workerQueueInfo = workerQueue.getWorkerQueueInfo( taskInfoList );
        boolean masterHasWork = masterQueue.hasWork();
        NodeUpdateInfo update = new NodeUpdateInfo( completionInfo, workerQueueInfo, Globals.localIbis.identifier(), masterHasWork );
        return update;
    }

    private void updateLocalGossip()
    {
        NodeUpdateInfo update = buildLocalUpdate();
        gossiper.registerGossip( update );
        NodeInfo nodeInfo = nodes.get( Globals.localIbis.identifier() );
        if( nodeInfo != null ) {
            nodeInfo.registerNodeInfo( update.workerQueueInfo, update.completionInfo );
        }
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * @param port The port to send the result to.
     * @param id The job instance identifier.
     * @param result The result to send.
     * @return <code>true</code> if the message could be sent.
     */
    private boolean sendResultMessage( IbisIdentifier port, JobInstanceIdentifier id, Object result )
    {
        Message msg = new JobResultMessage( id, result );	
        jobResultMessageCount.add();
        return sendPort.tryToSend( port, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
    }

    /**
     * A node has sent us an accept message, handle it.
     * @param m The update message.
     */
    private void handleGossipMessage( GossipMessage m )
    {
        if( Settings.traceNodeProgress || Settings.traceRegistration || Settings.traceGossip ){
            Globals.log.reportProgress( "Received gossip message " + m );
        }
        nodes.registerNodeUpdateInformation( m.gossip );
        gossiper.registerGossip( m.gossip );
        if( m.needsReply ) {
            gossiper.sendGossipReply( m.source );
        }
        synchronized( this ) {
            this.notify();
        }
    }

    /**
     * Handle a message containing a new task to run.
     * 
     * @param msg The message to handle.
     */
    private void handleRunTaskMessage( RunTaskMessage msg )
    {
        workerQueue.add( msg );
        IbisIdentifier source = msg.source;
        NodeInfo nodeInfo = nodes.get( source );
        boolean isDead = nodes.registerAsCommunicating( source );
        if( !isDead ) {
            postUpdateNodeMessage( source );
            if( nodeInfo != null ) {
                taskSources.add( nodeInfo );
            }
        }
        updateLocalGossip();
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * @param m The update message.
     */
    private void handleNodeUpdateInfo( NodeUpdateInfo m )
    {
        if( Settings.traceNodeProgress ){
            Globals.log.reportProgress( "Received node update message " + m );
        }
        NodeInfo nodeInfo = nodes.get( m.source );
        if( nodeInfo != null ) {
            nodeInfo.registerNodeInfo( m.workerQueueInfo, m.completionInfo );
            nodeInfo.registerAsCommunicating();
            if( m.masterHasWork ) {
                // A node that has work deserves a message about our capabilities.
                taskSources.add( nodeInfo );
            }
        }
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
        handleNodeUpdateInfo( m.update );
        gossiper.registerGossip( m.update );
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
        nodes.registerTaskCompleted( result );
        handledTaskCount.add();
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
        if( msg instanceof TaskCompletedMessage ) {
            handleTaskCompletedMessage( (TaskCompletedMessage) msg );
        }
        else if( msg instanceof JobResultMessage ) {
            JobResultMessage m = (JobResultMessage) msg;

            handleJobResultMessage( m );
        }
        else if( msg instanceof UpdateNodeMessage ) {
            handleNodeUpdateMessage( (UpdateNodeMessage) msg );
        }
        else if( msg instanceof GossipMessage ) {
            handleGossipMessage( (GossipMessage) msg );
        }
        else if( msg instanceof RunTaskMessage ){
            handleRunTaskMessage( (RunTaskMessage) msg );
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
            Service.waitToTerminate( t );
        }
        synchronized( this ) {
            stopTime = System.nanoTime();
        }
    }

    /** On a locked queue, try to send out as many task as we can. */
    private void drainMasterQueue()
    {
        if( drainingQueue.tryAcquire() ) {
            // Only bother to drain the queue if no other thread is working on it.
            LinkedList<Submission> submissions = masterQueue.getSubmissions( nodes );
            if( Settings.traceNodeProgress && !submissions.isEmpty() ){
                System.out.println( "Got " + submissions.size() + " submissions from master queue" );
            }
            drainingQueue.release();
            sendSubmissionsToWorkers( submissions );
        }
    }

    /**
     * @param submissions The list of submissions to send.
     */
    private void sendSubmissionsToWorkers( LinkedList<Submission> submissions )
    {
        while( !submissions.isEmpty() ) {
            Submission sub = submissions.removeFirst();
            NodeTaskInfo wti = sub.worker;
            TaskInstance task = sub.task;
            NodeInfo worker = wti.nodeInfo;
            long taskId = nextTaskId++;
            worker.registerTaskStart( task, taskId, sub.predictedDuration );
            if( Settings.traceMasterQueue ) {
                System.out.println( "Selected " + worker + " as best for task " + task );
            }

            RunTaskMessage msg = new RunTaskMessage( worker.ibis, task, taskId );
            boolean ok = sendPort.tryToSend( worker.ibis, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
            if( !ok ){
                // Try to put the paste back in the tube.
                // The send port has already registered the trouble.
                worker.retractTask( msg.taskId );
                masterQueue.add( msg.taskInstance );
            }
            submitMessageCount.add();
        }
    }

    /**
     * Adds the given task to the work queue of this master.
     * @param task The task instance to add to the queue.
     */
    void submit( TaskInstance task )
    {
        if( Settings.traceNodeProgress || Settings.traceMasterQueue ) {
            System.out.println( "Master: received task " + task );
        }
        masterQueue.add( task );
        drainMasterQueue();
        updateLocalGossip();
    }

    private boolean keepRunning()
    {
	boolean res = !stopped.isSet() || runningTasks.isAbove( 0 );
	return res;
    }

    /** Run a work thread. Only return when we want to shut down the node. */
    void runWorkThread()
    {
        while( keepRunning() ) {
            RunTaskMessage message = null;

            updateAdministration();
            updateLocalGossip();
            if( runningTasks.isBelow( numberOfProcessors ) ) {
        	// Only try to start a new task when there are idle
        	// processors.
                message = workerQueue.remove();
            }
            if( message == null ) {
                long sleepTime = 3;
                if( Settings.traceWaits ) {
                    System.out.println( "Worker: waiting for " + sleepTime + "ms for new tasks in queue" );
                }
                // Wait a little, there is nothing to do.
                try{
                    synchronized( this ) {
                        this.wait( sleepTime );
                    }
                }
                catch( InterruptedException e ){
                    // Not interesting.
                }
            }
            else {
                // We have a task to execute.
                long runMoment = System.nanoTime();
                TaskType type = message.taskInstance.type;
                long queueInterval = runMoment-message.getQueueMoment();
                int queueLength = message.getQueueLength();
                taskInfoList.setQueueTimePerTask( type, queueInterval, queueLength );
                Task task = jobs.getTask( type );

                runningTasks.up();
                if( Settings.traceNodeProgress ) {
                    System.out.println( "Worker: handed out task " + message + " of type " + type + "; it was queued for " + Service.formatNanoseconds( queueInterval ) + "; there are now " + runningTasks + " running tasks" );
                }
                Object input = message.taskInstance.input;
                if( task instanceof AtomicTask ) {
                    AtomicTask at = (AtomicTask) task;
                    Object result = at.run( input, this );
                    transferResult( message, result, runMoment );
                }
                else if( task instanceof MapReduceTask ) {
                    MapReduceTask mrt = (MapReduceTask) task;
                    MapReduceHandler handler = new MapReduceHandler( this, mrt, message, runMoment );
                    mrt.map( input, handler );
                    handler.start();
                }
                else if( task instanceof AlternativesTask ) {
                    // FIXME: implement this.
                }
                else {
                    Globals.log.reportInternalError( "Don't know what to do with a task of type " + task.getClass() );
                }
                if( Settings.traceNodeProgress ) {
                    System.out.println( "Work thread: completed " + message );
                }
            }
            synchronized( this ){
                // This thread has stopped. Wake up all others, since they
                // will want to stop too.
                this.notifyAll();
            }
        }
    }

    /**
     * @param message
     * @param result
     * @param runMoment
     */
    void transferResult( RunTaskMessage message, Object result, long runMoment )
    {
        long taskCompletionMoment = System.nanoTime();

        TaskType type = message.taskInstance.type;
        CompletionInfo[] completionInfo = masterQueue.getCompletionInfo( jobs, nodes, getIdleTasks() );
        WorkerQueueInfo[] workerQueueInfo = workerQueue.getWorkerQueueInfo( taskInfoList );
        long workerDwellTime = taskCompletionMoment-message.getQueueMoment();
        if( traceStats ) {
            double now = 1e-9*(System.nanoTime()-startTime);
            System.out.println( "TRACE:workerDwellTime " + type + " " + now + " " + 1e-9*workerDwellTime );
        }
        Message msg = new TaskCompletedMessage( message.taskId, workerDwellTime, completionInfo, workerQueueInfo );
        boolean ok = sendPort.tryToSend( message.source, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        taskResultMessageCount.add();

        // FIXME: try to do something if we couldn't send to the originator of the job. At least retry.

        TaskType nextTaskType = jobs.getNextTaskType( type );
        if( nextTaskType == null ){
            // This was the final step. Report back the result.
            JobInstanceIdentifier identifier = message.taskInstance.jobInstance;
            sendResultMessage( identifier.ibis, identifier, result );
        }
        else {
            // There is a next step to take.
            TaskInstance nextTask = new TaskInstance( message.taskInstance.jobInstance, nextTaskType, result );
            submit( nextTask );
        }

        // Update statistics.
        final long computeInterval = taskCompletionMoment-runMoment;
        taskInfoList.countTask( type, computeInterval );
        runningTasks.down();
        if( Settings.traceNodeProgress || Settings.traceRemainingJobTime ) {
            long queueInterval = runMoment-message.getQueueMoment();
            Globals.log.reportProgress( "Completed " + message.taskInstance + "; queueInterval=" + Service.formatNanoseconds( queueInterval ) + "; runningTasks=" + runningTasks );
        }
    }

    /**
     * Wait until at least the given number of nodes have been registered with this node.
     * Since nodes will never register themselves instantaneously with other nodes,
     * the first jobs that are submitted may be executed on the first available node, instead
     * of the best one. Waiting until there is some choice can therefore be an advantage.
     * Of course, it must be certain that the given number of nodes will ever join the computation.
     * @param n The number of nodes to wait for.
     * @param waittime The maximal time in milliseconds to wait for these nodes.
     * @return The actual number of nodes at the moment we stopped waiting.
     */
    public int waitForReadyNodes( int n, long waittime )
    {
        return nodes.waitForReadyNodes( n, waittime );
    }
}
