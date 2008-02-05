package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.LinkedList;

/**
 * A worker in the Maestro multiple master-worker system.
 * @author Kees van Reeuwijk
 */
@SuppressWarnings("synthetic-access")
public class Worker extends Thread implements WorkSource, PacketReceiveListener<MasterMessage> {
    private final PacketUpcallReceivePort<MasterMessage> receivePort;
    private final PacketSendPort<WorkerMessage> sendPort;
    private long queueEmptyMoment = 0L;
    private long queueEmptyInterval = 0L;
    private final LinkedList<RunJobMessage> queue = new LinkedList<RunJobMessage>();
    private final LinkedList<IbisIdentifier> unusedNeighbors = new LinkedList<IbisIdentifier>();
    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    private final WorkThread workThreads[] = new WorkThread[numberOfProcessors];
    private boolean stopped = false;
    private ReceivePortIdentifier exclusiveMaster = null;
    private boolean sawJobs = false;

    /**
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to.
     * @param master The master that jobs may submit new jobs to.
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis, Master master ) throws IOException
    {
        super( "Worker" );   // Create a thread with a name.
        unusedNeighbors.add( ibis.identifier() );
        receivePort = new PacketUpcallReceivePort<MasterMessage>( ibis, Globals.workerReceivePortName, this );
        sendPort = new PacketSendPort<WorkerMessage>( ibis );
        for( int i=0; i<numberOfProcessors; i++ ) {
            WorkThread t = new WorkThread( this, master );
            workThreads[i] = t;
            t.start();
        }
        if( Settings.traceNodes ){
            Globals.tracer.traceAlias( master.identifier(), receivePort.identifier() );
        }
        receivePort.enable();   // We're open for business.
    }

    private synchronized boolean isStopped()
    {
        return stopped;
    }

    /**
     * Returns the identifier of the job submission port of this worker.
     * @return The port identifier.
     */
    public ReceivePortIdentifier identifier()
    {
        return receivePort.identifier();
    }

    private IbisIdentifier getUnusedNeighbor()
    {
        IbisIdentifier res;

        synchronized( unusedNeighbors ){
            res = unusedNeighbors.pollFirst();
        }
        return res;
    }

    private void addNeighbors( IbisIdentifier l[] )
    {
        synchronized( unusedNeighbors ){
            for( IbisIdentifier n: l ){
                unusedNeighbors.add( n );
            }
        }
    }

    private void sendResignMessage( ReceivePortIdentifier master ) throws IOException
    {
        WorkerResignMessage msg = new WorkerResignMessage( receivePort.identifier() );
        if( Settings.traceNodes ) {
            Globals.tracer.traceSentMessage( msg, master );
        }
        sendPort.send(msg, master);
    }

    /**
     * Handle a message containing new neighbors.
     * 
     * @param msg The message to handle.
     */
    private void handleAddNeighborsMessage(AddNeighborsMessage msg)
    {
        addNeighbors( msg.getNeighbors() );
    }

    /**
     * Handle a message containing a new job to run.
     * 
     * @param msg The message to handle.
     */
    private void handleRunJobMessage( RunJobMessage msg )
    {
        msg.setQueueTime( System.nanoTime() );
        synchronized( queue ) {
            sawJobs = true;
            if( queueEmptyMoment>0 ){
                // Compute a queue empty interval from this moment.
                queueEmptyInterval = System.nanoTime() - queueEmptyMoment;
                queueEmptyMoment = 0L;
            }
            queue.add( msg );
            queue.notifyAll();
        }
    }

    /**
     * @param msg The ping message to handle.
     */
    private void handlePingMessage( PingMessage msg )
    {
        // First force the benchmark to be compiled.
        double benchmarkScore = msg.runBenchmark();
        long startTime = System.nanoTime();
    
        benchmarkScore = msg.runBenchmark();
        long benchmarkTime = System.nanoTime()-startTime;
        ReceivePortIdentifier master = msg.getMaster();
        PingReplyMessage m = new PingReplyMessage( receivePort.identifier(), workThreads.length, benchmarkScore, benchmarkTime );
        if( Settings.traceNodes ) {
            Globals.tracer.traceSentMessage( m, receivePort.identifier() );
        }
        try {
            sendPort.send( m, master );
        }
        catch( IOException x ){
            Globals.log.reportError( "Cannot send ping reply to master " + master );
            x.printStackTrace( Globals.log.getPrintStream() );
        }
    }

    /**
     * Handles job request message <code>msg</code>.
     * @param p The port on which the packet was received.
     * @param msg The job we received and will put in the queue.
     */
    public void packetReceived( PacketUpcallReceivePort<MasterMessage> p, MasterMessage msg )
    {
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Worker: received message " + msg );
        }
        if( Settings.traceNodes ) {
            Globals.tracer.traceReceivedMessage( msg, p.identifier() );
        }
        if( msg instanceof RunJobMessage ){
            RunJobMessage runJobMessage = (RunJobMessage) msg;
    
            handleRunJobMessage( runJobMessage );
        }
        else if( msg instanceof PingMessage ){
            PingMessage ping = (PingMessage) msg;
    
            handlePingMessage( ping );
        }
        else if( msg instanceof AddNeighborsMessage ){
            AddNeighborsMessage addMsg = (AddNeighborsMessage) msg;
    
            handleAddNeighborsMessage( addMsg );
        }
        else {
            Globals.log.reportInternalError( "FIXME: handle " + msg );
        }
        // Now send an unsubscribe message if we only handle work from a specific master.
        if( exclusiveMaster != null && !exclusiveMaster.equals( msg.source ) ){
            // Resign from the given master.
            try {
                sendResignMessage( msg.source );
            }
            catch( IOException x ){
                // Nothing we can do about it.
            }
        }
    }

    private boolean findNewMaster()
    {
        IbisIdentifier m = getUnusedNeighbor();
        if( m == null ){
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "No neighbors to ask for work" );
            }
            return false;
        }
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Asking neighbor " + m + " for work" );
        }
        try {
            WorkRequestMessage msg = new WorkRequestMessage( receivePort.identifier() );

            if( Settings.traceNodes ) {
                // FIXME: compute a receive port identifier for this one.
                Globals.tracer.traceSentMessage( msg, null );
            }
            sendPort.send( msg, m, Globals.masterReceivePortName );
        }
        catch( IOException x ){
            Globals.log.reportError( "Failed to send a work request message to neighbor " + m );
            x.printStackTrace();
        }
        return true;
    }

    /** Runs this worker. */
    @Override
    public void run()
    {
        System.out.println( "Starting worker thread" );
        for( int i=0; i<numberOfProcessors; i++ ) {
            Service.waitToTerminate( workThreads[i] );
            System.out.println( "Ended work thread " + i );
        }
        System.out.println( "End of worker thread" );
    }

    /**
     * Stop this worker.
     */
    public synchronized void setStopped()
    {
        stopped = true;
        synchronized( queue ) {
            queue.notifyAll();
        }
        System.out.println( "Worker is set to stopped" );
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis( IbisIdentifier theIbis )
    {
        synchronized( unusedNeighbors ) {
            unusedNeighbors.remove( theIbis );
        }
        // FIXME: remove any jobs from this ibis from our queue.
    }

    /**
     * A new ibis has joined the computation.
     * @param theIbis The ibis that has joined.
     */
    public void addIbis( IbisIdentifier theIbis )
    {
        synchronized( unusedNeighbors ){
            unusedNeighbors.add( theIbis );
        }
    }

    /**
     * Send resign messages to all masters except for the one given here.
     * @param identifier the master we shouldn't resign from.
     */
    public void workOnlyFor(ReceivePortIdentifier identifier)
    {
        exclusiveMaster = identifier;
    }

    /** Gets a job to execute.
     * @return The next job to execute.
     */
    @Override
    public RunJobMessage getJob()
    {
        while( true ) {
            try {
                synchronized( queue ) {
                    if( queue.isEmpty() ) {
                        if( sawJobs ){
                            queueEmptyMoment = System.nanoTime();
                        }
                        if( isStopped() ) {
                            // No jobs in queue, and worker is stopped. Tell
                            // return null to indicate that there won't be further
                            // jobs.
                            break;
                        }
                        if( !findNewMaster() ) {
                            System.out.println( "Worker: waiting for new jobs in queue" );
                            queue.wait();
                        }
                    }
                    else {
                        return queue.remove();
                    }

                }
            }
            catch( InterruptedException e ){
                // Not interesting.
            }
        }
        return null;
    }

    /** Reports the result of the execution of a job. (Overrides method in superclass.)
     * @param jobMessage The job to run.
     * @param r The returned value of this job.
     */
    @Override
    public void reportJobResult( RunJobMessage jobMessage, JobReturn r )
    {
        long computeTime = System.nanoTime()-jobMessage.getQueueTime();
        long interval;
        synchronized( queue ){
            interval = queueEmptyInterval;
            queueEmptyInterval = 0L;
        }
        try {
            long queueInterval = jobMessage.getRunTime()-jobMessage.getQueueTime();
            JobResultMessage msg = new JobResultMessage( receivePort.identifier(), r, jobMessage.getId(), computeTime, interval, queueInterval );
            if( Settings.traceNodes ) {
                Globals.tracer.traceSentMessage( msg, receivePort.identifier() );
            }
            sendPort.send( msg, jobMessage.getResultPort() );
            System.out.println( "Returned job result " + r + " for job "  + jobMessage );
        }
        catch( IOException x ){
            // Something went wrong in sending the result back.
            Globals.log.reportError( "Worker failed to send job result" );
            x.printStackTrace( Globals.log.getPrintStream() );
        }
    }

}
