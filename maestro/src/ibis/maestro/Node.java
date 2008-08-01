package ibis.maestro;

import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.Registry;
import ibis.ipl.RegistryEventHandler;
import ibis.maestro.UnregisteredNodeList.UnregisteredNodeInfo;

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
    private final PacketSendPort sendPort;
    final PacketUpcallReceivePort receivePort;
    final long startTime;
    private long stopTime = 0;
    private static final String MAESTRO_ELECTION_NAME = "maestro-election";

    private RegistryEventHandler registryEventHandler;

    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    private static final int workThreadCount = numberOfProcessors+2;
    private final WorkThread workThreads[] = new WorkThread[workThreadCount];
    private final UpdateThread updaters[] = new UpdateThread[numberOfProcessors];

    /** The list of ibises we haven't (successfully) registered with yet. */
    private final UnregisteredNodeList unregisteredNodes = new UnregisteredNodeList();

    private final TaskSources taskSources = new TaskSources();

    /** The incoming message queue. */
    private final MessageQueue messageQueue = new MessageQueue();

    /** The list of maestro nodes in this computation. */
    private final MaestroList maestros = new MaestroList();

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
    private Counter registrationMessageCount = new Counter();
    private Counter acceptMessageCount = new Counter();
    private Counter updateMessageCount = new Counter();
    private Counter submitMessageCount = new Counter();
    private Counter taskResultMessageCount = new Counter();
    private Counter jobResultMessageCount = new Counter();

    private Flag stopped = new Flag( false );

    private UpDownCounter runningTasks = new UpDownCounter();
    private final JobList jobs;
    private Semaphore drainingQueue = new Semaphore( 1, false );

    /**
     * Regularly send update messages to all neighbours.
     *
     * @author Kees van Reeuwijk.
     */
    private final class UpdateThread extends Thread {
        private ArrayList<NodeInfo> targets = new ArrayList<NodeInfo>();

        UpdateThread()
        {
            super( "updater thread" );
            setDaemon( true );
        }
        
        synchronized void addTarget( NodeInfo target )
        {
            targets.add( target );
            notify();
        }

        /** Runs this thread. (Overrides method in superclass.) */
        @SuppressWarnings("synthetic-access")
        @Override
        public void run()
        {
            int ix = 0;
            while( !stopped.isSet() ) {
                NodeInfo target = null;

                while( !targets.isEmpty() ) {
                    if( ix>=targets.size() ) {
                        ix = 0;
                    }
                    target = targets.get( ix++ );
                    if( !target.isDead() ) {
                        // We have a target.
                        break;
                    }
                    // The target is dead, remove it from our list.
                    targets.remove( target );
                    target = null;
                }
                // At this point we either have a target, or the list is empty, and target is null.
                try{
                    if( target == null ) {
                        // The list of targets is empty. Wake us when there is something to do.
                        synchronized( this ) {
                            this.wait();
                        }
                    }
                    else {
                        if( target.isReady() ) {
                            sendUpdateNodeMessage( target );
                        }
                        synchronized( this ) {
                            this.wait( Settings.UPDATE_INTERVAL );
                        }
                    }
                }
                catch( InterruptedException e ){
                    // Nothing.
                }
            }
        }
    }

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
            addUnregisteredNode( theIbis, local );
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
            removeNode( theIbis );
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
            removeNode( theIbis );
            removeNode( theIbis );
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
                maestros.addMaestro( new MaestroInfo( theIbis ) );
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
     * Returns true iff this node is a maestro.
     * @return True iff this node is a maestro.
     */
    public boolean isMaestro() { return isMaestro; }

    /** Registers the ibis with the given identifier as one that has left the
     * computation.
     * @param id The ibis that has left.
     */
    private void registerIbisLeft( IbisIdentifier id )
    {
        boolean noMaestrosLeft = maestros.remove( id );
        if( noMaestrosLeft ) {
            Globals.log.reportProgress( "No maestros left; stopping.." );
            setStopped();
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
        IbisIdentifier maestro;

        this.jobs = jobs;
        TaskType taskTypes[] = jobs.getSupportedTaskTypes();
        masterQueue = new MasterQueue( taskTypes );
        workerQueue = new WorkerQueue( taskTypes );
        taskInfoList.registerLocalTasks( taskTypes, jobs );
        ibisProperties.setProperty( "ibis.pool.name", "MaestroPool" );
        registryEventHandler = new NodeRegistryEventHandler();
        Globals.localIbis = IbisFactory.createIbis(
            ibisCapabilities,
            ibisProperties,
            true,
            registryEventHandler,
            PacketSendPort.portType,
            PacketUpcallReceivePort.portType,
            PacketBlockingReceivePort.portType
        );
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Created ibis " + Globals.localIbis );
        }
        Registry registry = Globals.localIbis.registry();
        if( runForMaestro ){
            maestro = registry.elect( MAESTRO_ELECTION_NAME );
            isMaestro = maestro.equals( Globals.localIbis.identifier() );
        }
        else {
            isMaestro = false;

        }
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Ibis " + Globals.localIbis.identifier() + ": isMaestro=" + isMaestro );
        }
        for( int i=0; i<workThreads.length; i++ ) {
            WorkThread t = new WorkThread( this );
            workThreads[i] = t;
            t.start();
        }
        receivePort = new PacketUpcallReceivePort( Globals.localIbis, Globals.receivePortName, this );
        sendPort = new PacketSendPort( Globals.localIbis, this );
        sendPort.setLocalListener( this );    // FIXME: no longer necessary
        this.traceStats = System.getProperty( "ibis.maestro.traceWorkerStatistics" ) != null;
        for( int i=0; i<updaters.length; i++ ) {
            UpdateThread updater = new UpdateThread();
            updater.start();
            updaters[i] = updater;
        }
        startTime = System.nanoTime();
        start();
        registry.enableEvents();
        if( Settings.traceNodes ) {
            Globals.log.log( "Started a Maestro node" );
        }
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

    /**
     * Do all updates of the node adminstration that we can.
     * 
     */
    private void updateAdministration()
    {
        drainMessageQueue();
        registerWithMaster();
        drainMasterQueue();
    }

    /**
     * Returns true iff this listener is associated with the given port.
     * @param port The port it should be associated with.
     * @return True iff this listener is associated with the port.
     */
    public boolean hasReceivePort( ReceivePortIdentifier port )
    {
        boolean res = port.equals( receivePort.identifier() );
        return res;
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

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    private void removeNode( IbisIdentifier theIbis )
    {
        ArrayList<TaskInstance> orphans = nodes.removeNode( theIbis );
        masterQueue.add( orphans );
    }

    private void addUnregisteredNode( IbisIdentifier theIbis, boolean local )
    {
        NodeInfo info = nodes.registerNode( theIbis, local );
        if( info.isReady() ) {
            // We already know everything there is to know about this node.
            // (Presumably because it registered itself with us.)
            return;
        }
        unregisteredNodes.add( info );
    }

    private void removeNode( NodeIdentifier theWorker )
    {
        ArrayList<TaskInstance> orphans = nodes.removeNode( theWorker );
        masterQueue.add( orphans );
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

    private boolean sendRegisterNodeMessage( UnregisteredNodeInfo ni )
    {
        boolean ok = true;
        if( Settings.traceWorkerList ) {
            Globals.log.reportProgress( "Node " + Globals.localIbis.identifier() + ": sending registration message to ibis " + ni );
        }
        TaskType taskTypes[] = jobs.getSupportedTaskTypes();
        RegisterNodeMessage msg = new RegisterNodeMessage( receivePort.identifier(), taskTypes, ni.ourIdentifierForNode );
        long sz = sendPort.tryToSend( ni.ibis, Globals.receivePortName, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( sz<0 ) {
            System.err.println( "Cannot register with node " + ni.ibis );
            ok = false;
        }
        registrationMessageCount.add();
        return ok;
    }

    private void sendUpdateNodeMessage( NodeIdentifier node, NodeIdentifier identifierOnNode )
    {
        CompletionInfo[] completionInfo = masterQueue.getCompletionInfo( jobs, nodes, getIdleTasks() );
        WorkerQueueInfo[] workerQueueInfo = workerQueue.getWorkerQueueInfo( taskInfoList );
        UpdateNodeMessage msg = new UpdateNodeMessage( identifierOnNode, completionInfo, workerQueueInfo );

        if( Settings.traceUpdateMessages ) {
            Globals.log.reportProgress( "Sending " + msg );
        }
        // We ignore the result because we don't care about the message size,
        // and if the update failed, it failed.
        sendPort.tryToSend( node, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
        updateMessageCount.add();
    }

    private int getIdleTasks()
    {
	return Math.max( numberOfProcessors-runningTasks.get(), 0 );
    }

    private void sendUpdateNodeMessage( NodeInfo node )
    {
        sendUpdateNodeMessage( node.ourIdentifierForNode, node.getTheirIdentifierForUs() );
    }

    private void sendAcceptNodeMessage( NodeInfo node, long sendTime )
    {
        AcceptNodeMessage msg = new AcceptNodeMessage( node.getTheirIdentifierForUs(), sendTime );
	
	if( Settings.traceUpdateMessages ) {
	    Globals.log.reportProgress( "Sending " + msg );
	}
	sendPort.tryToSend( node.ourIdentifierForNode, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
	acceptMessageCount.add();
    }

    /**
     * A worker has sent us a message to register itself with us. This is
     * just to tell us that it's out there. We tell it what our receive port is,
     * and which handle we have assigned to it, so that it can then inform us
     * of the types of tasks it supports.
     * @param m The worker registration message.
     */
    private void handleRegisterNodeMessage( RegisterNodeMessage m )
    {
        NodeIdentifier theirIdentifierForUs = m.senderIdentifierForReceiver;  // Change of perspective...

        if( Settings.traceNodeProgress || Settings.traceRegistration ){
            Globals.log.reportProgress( "received registration message from node " + m.port );
        }
        if( m.supportedTypes.length == 0 ) {
            Globals.log.reportInternalError( "Node " + m.port + " has zero supported types??" );
        }
        NodeInfo nodeInfo = nodes.subscribeNode( m.port, m.supportedTypes, theirIdentifierForUs, sendPort );
        // FIXME: support retries.
        sendAcceptNodeMessage( nodeInfo, m.sendMoment );
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
        final NodeInfo mi = nodes.get( result.source );
        if( mi != null ) {
            taskSources.add( mi );
        }
        handledTaskCount.add();
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
        final NodeInfo mi = nodes.get( m.source );
        if( mi != null ) {
            taskSources.add( mi );
        }
        nodes.registerCompletionInfo( m.source, m.workerQueueInfo, m.completionInfo );
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * @param m The update message.
     */
    private void handleAcceptNodeMessage( AcceptNodeMessage m )
    {
        if( Settings.traceNodeProgress || Settings.traceRegistration ){
            Globals.log.reportProgress( "Received node accept message " + m );
        }
        final NodeInfo nodeInfo = nodes.get( m.source );
        long pingTime = m.arrivalMoment-m.sendMoment;
        if( Settings.traceRegistration ){
            Globals.log.reportProgress( "Ping time to " + m.source + " is " + Service.formatNanoseconds( pingTime ) );
        }
        nodeInfo.setPingTime( pingTime );
        for( UpdateThread updater: updaters ) {
            updater.addTarget( nodeInfo );
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
        boolean isDead = nodes.registerAsCommunicating( msg.source );
        if( !isDead ) {
            NodeInfo nodeInfo = nodes.get( msg.source );
            if( nodeInfo != null ) {
                // We have the node in our administration.
                sendUpdateNodeMessage( nodeInfo );
            }
        }
    }

    /**
     * Handles message <code>msg</code> from worker.
     * @param msg The message we received.
     */
    @Override
    public void messageReceived( Message msg )
    {
        messageQueue.add( msg );
    }

    /** Handle the given message. */
    private void handleMessage( Message msg )
    {
        if( Settings.traceNodeProgress ){
            Globals.log.reportProgress( "Received message " + msg );
        }
        if( msg instanceof TaskCompletedMessage ) {
            TaskCompletedMessage result = (TaskCompletedMessage) msg;

            handleTaskCompletedMessage( result );
        }
        else if( msg instanceof JobResultMessage ) {
            JobResultMessage m = (JobResultMessage) msg;

            reportCompletion( m.job, m.result );
        }
        else if( msg instanceof UpdateNodeMessage ) {
            handleNodeUpdateMessage( (UpdateNodeMessage) msg );
        }
        else if( msg instanceof RegisterNodeMessage ) {
            handleRegisterNodeMessage( (RegisterNodeMessage) msg );
        }
        else if( msg instanceof AcceptNodeMessage ) {
            handleAcceptNodeMessage( (AcceptNodeMessage) msg );
        }
        else if( msg instanceof NodeResignMessage ) {
            NodeResignMessage m = (NodeResignMessage) msg;

            removeNode( m.source );
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
    
    private void drainMessageQueue()
    {
        while( true ) {
            Message msg = messageQueue.removeIfAny();
            if( msg == null ) {
                return;
            }
            handleMessage( msg );
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

    /**
     * If there is any new master on our list, try to register with it.
     */
    private void registerWithMaster()
    {
        UnregisteredNodeInfo ni = unregisteredNodes.removeIfAny();
        if( ni != null ) {
            if( Settings.traceNodeProgress ){
                Globals.log.reportProgress( "registering with node " + ni );
            }
            boolean ok = sendRegisterNodeMessage( ni );
            if( !ok ) {
                int tries = ni.incrementTries();
                if( tries<Settings.MAXIMAL_REGISTRATION_TRIES ) {
                    unregisteredNodes.add( ni );
                }
                else {
                    Globals.log.reportError( "I cannot register with node " + ni.ibis + " even after " + Settings.MAXIMAL_REGISTRATION_TRIES + " attempts; giving up" );
                }
            }
        }
    }

    private void askMoreWork()
    {
        if( isStopped() ) {
            return;
        }
        // Try to tell a known master we want more tasks. We do this by
        // telling it about our current state.
        NodeInfo taskSource = taskSources.getRandomWorkSource();
        if( taskSource != null ){
            if( Settings.traceNodeProgress ){
                Globals.log.reportProgress( "Asking node " + taskSource.ourIdentifierForNode + " for work" );
            }
            if( isStopped() ) {
                return;
            }
            sendUpdateNodeMessage( taskSource );
            return;
        }
        if( Settings.noStealRequests ){
            return;
        }

        // Finally, just tell a random master about our current work queues.
        taskSource = nodes.getRandomReadyNode();
        if( taskSource != null ){
            if( Settings.traceNodeProgress ){
                Globals.log.reportProgress( "Updating node " + taskSource.ourIdentifierForNode );
            }
            if( isStopped() ) {
                return;
            }
            sendUpdateNodeMessage( taskSource );
        }
    }

    /**
     * Send a result message to the given port, using the given job identifier
     * and the given result value.
     * @param port The port to send the result to.
     * @param id The job instance identifier.
     * @param result The result to send.
     * @return The size of the sent message, or -1 if the transmission failed.
     */
    private long sendResultMessage( ReceivePortIdentifier port, JobInstanceIdentifier id, Object result )
    {
        Message msg = new JobResultMessage( id, result );	
        jobResultMessageCount.add();
        return sendPort.tryToSend( port, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
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
    private void sendSubmissionsToWorkers(LinkedList<Submission> submissions) {
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

            RunTaskMessage msg = new RunTaskMessage( worker.getTheirIdentifierForUs(), worker.ourIdentifierForNode, task, taskId );
            long sz = sendPort.tryToSend( worker.ourIdentifierForNode, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
            if( sz<0 ){
                // Try to put the paste back in the tube.
                // The sendport has already registered the trouble.
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
            updateAdministration();
            RunTaskMessage message = null;
            if( runningTasks.isLessOrEqual( numberOfProcessors ) ) {
                message = workerQueue.remove();
            }
            if( message == null ) {
                if( false ) {
                    // TODO: remove this code, or enable again.
                    if( runningTasks.isBelow( numberOfProcessors ) ) {
                        // No work in the worker queue, not all processors are busy, See if we can get more work.
                        askMoreWork();
                    }
                }
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
    void transferResult( RunTaskMessage message, Object result, long runMoment ) {
        long taskCompletionMoment = System.nanoTime();
        final NodeIdentifier masterId = message.source;

        TaskType type = message.taskInstance.type;
        CompletionInfo[] completionInfo = masterQueue.getCompletionInfo( jobs, nodes, getIdleTasks() );
        WorkerQueueInfo[] workerQueueInfo = workerQueue.getWorkerQueueInfo( taskInfoList );
        long workerDwellTime = taskCompletionMoment-message.getQueueMoment();
        if( traceStats ) {
            double now = 1e-9*(System.nanoTime()-startTime);
            System.out.println( "TRACE:workerDwellTime " + type + " " + now + " " + 1e-9*workerDwellTime );
        }
        Message msg = new TaskCompletedMessage( message.workerIdentifier, message.taskId, workerDwellTime, completionInfo, workerQueueInfo );
        long sz = sendPort.tryToSend( masterId, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        taskResultMessageCount.add();

        // FIXME: try to do something if we couldn't send to the originator of the job. At least retry.

        TaskType nextTaskType = jobs.getNextTaskType( type );
        if( nextTaskType == null ){
            // This was the final step. Report back the result.
            JobInstanceIdentifier identifier = message.taskInstance.jobInstance;
            sendResultMessage( identifier.receivePort, identifier, result );
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
     */
    public void waitForReadyNodes( int n )
    {
        nodes.waitForReadyNodes( n );
    }
}
