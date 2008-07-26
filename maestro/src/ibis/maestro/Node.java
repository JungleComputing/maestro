package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.Registry;
import ibis.ipl.RegistryEventHandler;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

/**
 * A node in the Maestro dataflow network.
 * 
 * @author Kees van Reeuwijk
 *
 */
public final class Node extends Thread implements PacketReceiveListener<Message>
{
    static final IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.MEMBERSHIP_UNRELIABLE, IbisCapabilities.ELECTIONS_STRICT );
    private final Ibis ibis;
    private final PacketSendPort<Message> sendPort;
    final PacketUpcallReceivePort<Message> receivePort;
    final long startTime;
    private long activeTime = 0L;
    private long stopTime = 0;
    private static final String MAESTRO_ELECTION_NAME = "maestro-election";

    private RegistryEventHandler registryEventHandler;

    private ArrayList<WorkerTaskStats> taskStats = new ArrayList<WorkerTaskStats>();

    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    private final ArrayList<WorkThread> workThreads = new ArrayList<WorkThread>();

    /** The estimated time it takes to send an administration message. */
    private final TimeEstimate infoSendTime = new TimeEstimate( Service.MICROSECOND_IN_NANOSECONDS );

    /** The list of ibises we haven't registered with yet. */
    private final MasterInfoList unregisteredNodes = new MasterInfoList();

    private final TaskSources taskSources = new TaskSources();

    /** The list of maestro nodes in this computation. */
    private final MaestroList maestros = new MaestroList();

    /**
     * The list of nodes we want to accept. 
     */
    private final AcceptList acceptList = new AcceptList();

    /** The list of running jobs with their completion listeners. */
    private final RunningJobs runningJobs = new RunningJobs();

    /** The list of nodes we know about. */
    private final NodeList nodes = new NodeList();

    private boolean isMaestro;
    private boolean askMastersForWork = true;
    private final boolean traceStats;
    private final MasterQueue masterQueue = new MasterQueue();
    private final WorkerQueue workerQueue = new WorkerQueue();
    private long nextTaskId = 0;
    private long incomingTaskCount = 0;
    private long handledTaskCount = 0;
    private boolean stopped = false;
    private int runningTasks = 0;
    final JobList jobs;

    private class NodeRegistryEventHandler implements RegistryEventHandler {
        /**
         * A new Ibis joined the computation.
         * @param theIbis The ibis that joined the computation.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public void joined( IbisIdentifier theIbis )
        {
            boolean local = theIbis.equals( ibis.identifier() );
            if( !local ) {
                addUnregisteredNode( theIbis, local );
            }
        }

        /**
         * An ibis has died.
         * @param theIbis The ibis that died.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public void died( IbisIdentifier theIbis )
        {
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
        ibisProperties.setProperty( "ibis.pool.name", "MaestroPool" );
        registryEventHandler = new NodeRegistryEventHandler();
        ibis = IbisFactory.createIbis(
            ibisCapabilities,
            ibisProperties,
            true,
            registryEventHandler,
            PacketSendPort.portType,
            PacketUpcallReceivePort.portType,
            PacketBlockingReceivePort.portType
        );
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Created ibis " + ibis );
        }
        Registry registry = ibis.registry();
        if( runForMaestro ){
            maestro = registry.elect( MAESTRO_ELECTION_NAME );
            isMaestro = maestro.equals( ibis.identifier() );
        }
        else {
            isMaestro = false;

        }
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Ibis " + ibis.identifier() + ": isMaestro=" + isMaestro );
        }
        for( int i=0; i<numberOfProcessors; i++ ) {
            WorkThread t = new WorkThread( this );
            workThreads.add( t );
            t.start();
        }
        receivePort = new PacketUpcallReceivePort<Message>( ibis, Globals.receivePortName, this );
        sendPort = new PacketSendPort<Message>( ibis, this );
        sendPort.setLocalListener( this );    // FIXME: no longer necessary
        this.traceStats = System.getProperty( "ibis.maestro.traceWorkerStatistics" ) != null;
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
            Globals.log.reportProgress( "Set node to stopped state. Telling worker..." );
        }
        stopAskingForWork();
    }

    /**
     * Wait for this node to finish.
     */
    public void waitToTerminate()
    {
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Waiting for master to terminate" );
        }
        /**
         * Everything interesting happens in the master and worker.
         * So all we do here is wait for the master and worker to terminate.
         * We only stop this thread if both are terminated, so we can just wait
         * for one to terminate, and then the other.
         */
        Service.waitToTerminate( this );
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "master is terminated; waiting for worker to terminate" );
        }

        // FIXME: do termination properly!!

        /** Once the master has stopped, stop the worker. */
        waitForWorkThreadsToTerminate();
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "worker is terminated" );
        }
        printStatistics( System.out );
        try {
            ibis.end();
        }
        catch( IOException x ) {
            // Nothing we can do about it.
        }
        if( Settings.traceNodes ) {
            Globals.log.reportProgress( "Node has terminated" );
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

    /**
     * @return The ibis identifier of this node.
     */
    private IbisIdentifier ibisIdentifier()
    {
        return ibis.identifier();
    }

    /** This ibis was reported as 'may be dead'. Try
     * not to communicate with it.
     * @param theIbis The ibis that may be dead.
     */
    void setSuspect( IbisIdentifier theIbis )
    {
        try {
            ibis.registry().assumeDead( theIbis );
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
    void updateAdministration()
    {
        NodeIdentifier workerToAccept = null;

        workerToAccept = acceptList.removeIfAny();
        if( workerToAccept != null ) {
            boolean ok = sendAcceptMessage( workerToAccept );
            if( !ok ) {
                // Couldn't send an accept message: back on the list.
                acceptList.add( workerToAccept );
            }
        }
        drainQueue();
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
        addUnregisteredNode( ibis.identifier(), true );
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
        NodeInfo info = nodes.addNode( theIbis, local );
        unregisteredNodes.add( info );
    }

    private void removeNode( NodeIdentifier theWorker )
    {
        ArrayList<TaskInstance> orphans = nodes.removeNode( theWorker );
        masterQueue.add( orphans );
    }

    /** Print some statistics about the entire worker run. */
    void printStatistics( PrintStream s )
    {
        if( stopTime<startTime ) {
            System.err.println( "Node didn't stop yet" );
            stopTime = System.nanoTime();
        }
        s.printf(  "# threads       = %5d\n", workThreads.size() );
        nodes.printStatistics( s );
        jobs.printStatistics( s );
        sendPort.printStatistics( s, "send port" );
        if( activeTime<startTime ) {
            System.err.println( "Worker was not used" );
            activeTime = startTime;
        }
        long workInterval = stopTime-activeTime;
        workerQueue.printStatistics( s, workInterval );
        for( WorkerTaskStats stats: taskStats ) {
            if( stats != null ) {
                stats.reportStats( s, workInterval );
            }
        }
        s.println( "Worker: run time        = " + Service.formatNanoseconds( workInterval ) );
        s.println( "Worker: activated after = " + Service.formatNanoseconds( activeTime-startTime ) );
        masterQueue.printStatistics( s );
        s.printf(  "Master: # incoming tasks = %5d\n", incomingTaskCount );
        s.printf(  "Master: # handled tasks  = %5d\n", handledTaskCount );
    }

    private boolean sendAcceptMessage( NodeIdentifier workerID )
    {
        ReceivePortIdentifier myport = receivePort.identifier();
        NodeIdentifier idOnWorker = nodes.getNodeIdentifier( workerID );
        NodeAcceptMessage msg = new NodeAcceptMessage( idOnWorker, myport, workerID );
        boolean ok = true;
    
        nodes.setPingStartMoment( workerID );
        long sz = sendPort.tryToSend( workerID.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( sz<0 ){
            ok = false;
        }
        return ok;
    }

    private boolean registerWithNode( NodeInfo info )
    {
        boolean ok = true;
        if( Settings.traceWorkerList ) {
            Globals.log.reportProgress( "Node " + ibisIdentifier() + ": sending registration message to ibis " + info );
        }
        TaskType taskTypes[] = jobs.getSupportedTaskTypes();
        RegisterNodeMessage msg = new RegisterNodeMessage( receivePort.identifier(), taskTypes, info.localIdentifier );
        long sz = sendPort.tryToSend( info.ibis, Globals.receivePortName, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( sz<0 ) {
            System.err.println( "Cannot register with node " + info.ibis );
            setSuspect( info.ibis );
            ok = false;
        }
        return ok;
    }

    private void sendUpdateMessage( NodeIdentifier node, NodeIdentifier identifierOnNode )
    {
        long start = System.nanoTime();
        CompletionInfo[] completionInfo = masterQueue.getCompletionInfo( jobs, nodes );
        WorkerQueueInfo[] workerQueueInfo = workerQueue.getWorkerQueueInfo( taskStats );
        NodeUpdateMessage msg = new NodeUpdateMessage( identifierOnNode, completionInfo, workerQueueInfo );

        // We ignore the result because we don't care about the message size,
        // and if the update failed, it failed.
        sendPort.tryToSend( node.value, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
        infoSendTime.addSample( System.nanoTime()-start );
    }

    private void sendUpdate( NodeInfo node )
    {
        sendUpdateMessage( node.localIdentifier, node.getIdentifierOnNode() );
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
        NodeIdentifier workerID;

        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master: received registration message " + m + " from worker " + m.port );
        }
        if( m.supportedTypes.length == 0 ) {
            Globals.log.reportInternalError( "Node " + m.port + " has zero supported types??" );
        }
        workerID = nodes.subscribeNode( m.port, m.supportedTypes, m.masterIdentifier );
        if( workerID != null ) {
            sendPort.registerDestination( m.port, workerID.value );
            acceptList.add( workerID );
        }
    }

    /**
     * A worker has sent use a completion message for a task. Process it.
     * @param result The status message.
     * @param arrivalMoment FIXME
     */
    private void handleTaskCompletedMessage( TaskCompletedMessage result, long arrivalMoment )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received a worker task completed message " + result );
        }
        nodes.registerTaskCompleted( result, arrivalMoment );
        nodes.registerAsCommunicating( result.source );
        synchronized( this ) {
            handledTaskCount++;
        }
    }

    /**
     * A worker has sent us a message with its current status, handle it.
     * @param m The update message.
     * @param arrivalMoment The time in ns the message arrived.
     */
    private void handleWorkerUpdateMessage( NodeUpdateMessage m, long arrivalMoment )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received worker update message " + m );
        }
        nodes.registerCompletionInfo( m.source, m.workerQueueInfo, m.completionInfo, arrivalMoment );
    }

    private void handleNodeAcceptMessage( NodeAcceptMessage msg )
    {    
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Received a node accept message " + msg );
        }
        sendPort.registerDestination( msg.port, msg.source.value );
        nodes.registerAccept( msg.source, msg.port, msg.identifierOnNode );
        sendUpdateMessage( msg.source, msg.identifierOnNode );
    }

    /**
     * Handle a message containing a new task to run.
     * 
     * @param msg The message to handle.
     * @param arrivalMoment The moment in ns this message arrived.
     */
    private void handleRunTaskMessage( RunTaskMessage msg, long arrivalMoment )
    {
        if( activeTime == 0L ) {
            activeTime = arrivalMoment;
        }
        workerQueue.add( msg, arrivalMoment );
        boolean isDead = nodes.registerAsCommunicating( msg.source );
        if( !isDead ) {
            sendUpdate( nodes.get( msg.source ) );
        }
    }

    /**
     * Handles message <code>msg</code> from worker.
     * @param msg The message we received.
     */
    @Override
    public void messageReceived( Message msg, long arrivalMoment )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master: received message " + msg );
        }
        if( msg instanceof TaskCompletedMessage ) {
            TaskCompletedMessage result = (TaskCompletedMessage) msg;
    
            nodes.setUnsuspect( result.source );
            handleTaskCompletedMessage( result, arrivalMoment );
        }
        else if( msg instanceof JobResultMessage ) {
            JobResultMessage m = (JobResultMessage) msg;
    
            reportCompletion( m.job, m.result );
        }
        else if( msg instanceof NodeUpdateMessage ) {
            NodeUpdateMessage m = (NodeUpdateMessage) msg;
    
            nodes.setUnsuspect( m.source );
            handleWorkerUpdateMessage( m, arrivalMoment );
        }
        else if( msg instanceof RegisterNodeMessage ) {
            RegisterNodeMessage m = (RegisterNodeMessage) msg;
    
            handleRegisterNodeMessage( m );
        }
        else if( msg instanceof NodeResignMessage ) {
            NodeResignMessage m = (NodeResignMessage) msg;
    
            removeNode( m.source );
        }
        if( msg instanceof RunTaskMessage ){
            RunTaskMessage runTaskMessage = (RunTaskMessage) msg;
    
            handleRunTaskMessage( runTaskMessage, arrivalMoment );
        }
        else if( msg instanceof NodeAcceptMessage ) {
            NodeAcceptMessage am = (NodeAcceptMessage) msg;
    
            handleNodeAcceptMessage( am );
        }
        else {
            Globals.log.reportInternalError( "the master should handle message of type " + msg.getClass() );
        }
    }

    private void waitForWorkThreadsToTerminate()
    {
        WorkThread t = null;
        while( true ){
            synchronized( workThreads ){
                if( t != null ){
                    workThreads.remove( t );  // Remove a terminated worker.
                }
                if( workThreads.isEmpty() ){
                    break;
                }
                t = workThreads.get( 0 );
            }
            Service.waitToTerminate( t );
        }
        stopTime = System.nanoTime();
    }

    void stopWorker( WorkThread thread )
    {
        thread.shutdown();
        // It may linger a bit, we don't care.
        workThreads.remove( thread );
    }

    WorkThread spawnExtraWorker()
    {
        WorkThread t = new WorkThread( this );
        workThreads.add( t );
        t.start();
        return t;
    }

    /**
     * Given a task type, return the job it belongs to, or <code>null</code> if we
     * cannot find it. Since that is an internal error, report that error.
     * @param type
     * @return
     */
    private Job findJob( TaskType type )
    {
        int ix = type.job.searchJob( this );
        if( ix<0 ) {
            Globals.log.reportInternalError( "Unknown job id in task type " + type );
            return null;
        }
        return jobs.get( ix );
    }

    /**
     * Given a task type, return the task.
     * @param type The task type.
     * @return The task.
     */
    private Task findTask( TaskType type )
    {
        Job t = findJob( type );
        return t.tasks[type.taskNo];
    }

    /** Removes and returns a random task source from the list of
     * known task sources. Returns null if the list is empty.
     * 
     * @return The task source, or null if there isn't one.
     */
    private NodeInfo getRandomWorkSource()
    {
        synchronized( this ) {
            if( !askMastersForWork ){
                return null;
            }
        }
        return taskSources.getRandomWorkSource();
    }

    /**
     * Returns a random registered master.
     * @return The task source, or <code>null</code> if there isn't one.
     */
    private NodeInfo getRandomRegisteredMaster()
    {
        synchronized( this ) {
            if( !askMastersForWork ){
                return null;
            }
        }

        return nodes.getRandomRegisteredMaster();
    }

    /**
     * If there is any new master on our list, try to register with it.
     */
    void registerWithAnyMaster()
    {
        NodeInfo newIbis = unregisteredNodes.removeIfAny();
        if( newIbis != null ){
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Worker: registering with master " + newIbis );
            }
            // We record the transmission time as a reasonable estimate of a sleep time.
            long start = System.nanoTime();
            boolean ok = registerWithNode( newIbis );
            if( ok ) {
                infoSendTime.addSample( System.nanoTime()-start );
            }
            else {
                // We couldn't reach this master. Put it back on the list.
                unregisteredNodes.add( newIbis );
            }
            return;
        }
    }

    void askMoreWork()
    {
        // Try to tell a known master we want more tasks. We do this by
        // telling it about our current state.
        NodeInfo taskSource = getRandomWorkSource();
        if( taskSource != null ){
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Worker: asking master " + taskSource.localIdentifier + " for work" );
            }
            sendUpdate( taskSource );
            return;
        }

        // Finally, just tell a random master about our current work queues.
        taskSource = getRandomRegisteredMaster();
        if( taskSource != null ){
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Worker: updating master " + taskSource.localIdentifier );
            }
            sendUpdate( taskSource );
            return;
        }
    }

    /** Reports the result of the execution of a task. (Overrides method in superclass.)
     * @param task The task that was run.
     * @param result The result coming from the run task.
     */
    public void reportTaskCompletion( RunTask task, Object result )
    {
        long taskCompletionMoment = System.nanoTime();
        TaskType taskType = task.message.task.type;
        Job t = findJob( taskType );
        int nextTaskNo = taskType.taskNo+1;
        final NodeIdentifier masterId = task.message.source;

        CompletionInfo[] completionInfo = masterQueue.getCompletionInfo( jobs, nodes );
        WorkerQueueInfo[] workerQueueInfo = workerQueue.getWorkerQueueInfo( taskStats );
        long workerDwellTime = taskCompletionMoment-task.message.getQueueMoment();
        if( traceStats ) {
            double now = 1e-9*(System.nanoTime()-startTime);
            System.out.println( "TRACE:workerDwellTime " + taskType + " " + now + " " + 1e-9*workerDwellTime );
        }
        Message msg = new TaskCompletedMessage( task.message.workerIdentifier, task.message.taskId, workerDwellTime, completionInfo, workerQueueInfo );
        long sz = sendPort.tryToSend( masterId.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );

        // FIXME: try to do something if we couldn't send to the originator of the job. At least retry.

        if( nextTaskNo<t.tasks.length ){
            // There is a next step to take.
            TaskType nextTaskType = t.getNextTaskType( taskType );
            TaskInstance nextTask = new TaskInstance( task.message.task.jobInstance, nextTaskType, result );
            submit( nextTask );
        }
        else {
            // This was the final step. Report back the result.
            JobInstanceIdentifier identifier = task.message.task.jobInstance;
            sendResultMessage( identifier.receivePort, identifier, result );
        }

        long queueInterval = task.message.getRunMoment()-task.message.getQueueMoment();

        // Update statistics and notify our own queue waiters that something
        // has happened.
        final NodeInfo mi = nodes.get( masterId );
        taskSources.add( mi );
        synchronized( workerQueue ) {
            WorkerTaskStats stats = getWorkerTaskStats( taskType );
            stats.countTask( queueInterval, taskCompletionMoment-task.message.getRunMoment() );
        }
        runningTasks--;
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Completed " + task.message.task + "; queueInterval=" + Service.formatNanoseconds( queueInterval ) + "; runningTasks=" + runningTasks );
        }
        if( Settings.traceWorkerProgress ) {
            System.out.println( "Completed task "  + task.message );
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
        return sendPort.tryToSend( port, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
    }

    WorkerTaskStats getWorkerTaskStats( TaskType type )
    {
        int ix = type.index;
        WorkerTaskStats res;

        while( ix>=taskStats.size() ) {
            taskStats.add( null );
        }
        res = taskStats.get( ix );
        if( res == null ){
            res = new WorkerTaskStats( type );
            taskStats.set( ix, res );
        }
        return res;
    }
    /**
     * Tells this worker not to ask for work any more.
     */
    private void stopAskingForWork()
    {
        if( Settings.traceWorkerProgress ) {
            Globals.log.reportProgress( "Worker: don't ask for work" );
        }
        synchronized( this ){
            askMastersForWork = false;
        }
    }

    /** Gets a task to execute.
     * @return The next task to execute.
     */
    RunTask getTask()
    {
        while( true ) {
            boolean askForWork = false;
            registerWithAnyMaster();
            try {
                synchronized( workerQueue ) {
                    if( workerQueue.isEmpty() ) {
                        if( workerQueue.queueEmptyMoment == 0 ) {
                            workerQueue.queueEmptyMoment = System.nanoTime();
                        }
                        if( stopped && runningTasks == 0 ) {
                            // No tasks in queue, and worker is stopped. Return null to
                            // indicate that there won't be further tasks.
                            break;
                        }
                        if( taskSources.isEmpty() ){
                            // There was no master to subscribe to, update, or ask for work.
                            if( Settings.traceWorkerProgress || Settings.traceWaits ) {
                                System.out.println( "Worker: waiting for new tasks in queue" );
                            }
                            // Wait a little if there is nothing to do.
                            workerQueue.wait( (infoSendTime.getAverage()*2)/Service.MILLISECOND_IN_NANOSECONDS );
                        }
                        else {
                            askForWork = true;
                        }
                    }
                    else {
                        long now = System.nanoTime();
                        runningTasks++;
                        RunTaskMessage message = workerQueue.remove();
                        TaskType type = message.task.type;
                        message.setRunMoment( now );
                        long queueTime = now-message.getQueueMoment();
                        int queueLength = message.getQueueLength();
                        WorkerTaskStats stats = getWorkerTaskStats( type );
                        stats.setQueueTimePerTask( queueTime/(queueLength+1) );
                        Task task = findTask( message.task.type );
                        if( Settings.traceWorkerProgress ) {
                            System.out.println( "Worker: handed out task " + message + " of type " + type + "; it was queued for " + Service.formatNanoseconds( queueTime ) + "; there are now " + runningTasks + " running tasks" );
                        }
                        return new RunTask( task, message );
                    }
                }
                if( askForWork ){
                    askMoreWork();
                }
            }
            catch( InterruptedException e ){
                // Not interesting.
            }
        }
        return null;
    }

    /** On a locked queue, try to send out as many task as we can. */
    private void drainQueue()
    {
        synchronized( masterQueue ) {
            Submission sub = new Submission();
            long taskId;
            int reserved = 0;  // How many tasks are reserved for future submission.
            HashSet<TaskType> noReadyWorkers = new HashSet<TaskType>();

            if( Settings.traceMasterProgress ){
                System.out.println( "Master: submitting all possible tasks" );
            }
            nodes.resetReservations();   // FIXME: store reservations in a separate structure.
            while( true ) {
                if( masterQueue.isEmpty() ) {
                    // Mission accomplished.
                    return;
                }
                reserved = masterQueue.selectSubmisson( reserved, sub, nodes, noReadyWorkers );
                NodeTaskInfo wti = sub.worker;
                TaskInstance task = sub.task;
                if( wti == null ){
                    break;
                }
                NodeInfo worker = wti.nodeInfo;
                taskId = nextTaskId++;
                worker.registerTaskStart( task, taskId, sub.predictedDuration );
                if( Settings.traceMasterQueue ) {
                    System.out.println( "Selected " + worker + " as best for task " + task );
                }

                RunTaskMessage msg = new RunTaskMessage( worker.getIdentifierOnNode(), worker.localIdentifier, task, taskId );
                long sz = sendPort.tryToSend( worker.localIdentifier.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
                if( sz<0 ){
                    // Try to put the paste back in the tube.
                    // The sendport has already reported the trouble with the worker.
                    worker.retractTask( msg.taskId );
                    masterQueue.add( msg.task );
                }
            }
        }
    }

    /**
     * Adds the given task to the work queue of this master.
     * @param task The task instance to add to the queue.
     */
    void submit( TaskInstance task )
    {
        if( Settings.traceMasterProgress || Settings.traceMasterQueue ) {
            System.out.println( "Master: received task " + task );
        }
        synchronized ( masterQueue ) {
            incomingTaskCount++;
            masterQueue.add( task );
        }
        drainQueue();
    }

}
