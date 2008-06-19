package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.LinkedList;

/**
 * A master in the Maestro flow graph framework.
 * 
 * @author Kees van Reeuwijk
 * 
 */
@SuppressWarnings("synthetic-access")
public class Master extends Thread implements PacketReceiveListener<WorkerMessage>
{
    private final Node node;
    private final WorkerList workers = new WorkerList();
    private final PacketUpcallReceivePort<WorkerMessage> receivePort;
    private final PacketSendPort<MasterMessage> sendPort;
    private final MasterQueue queue;
    private final LinkedList<WorkerIdentifier> workersToAccept = new LinkedList<WorkerIdentifier>();

    private boolean stopped = false;
    private long nextJobId = 0;
    private long incomingJobCount = 0;
    private long handledJobCount = 0;
    private final long startTime;
    private long stopTime = 0;

    /**
     * A worker identifier.
     * This is in essence just an int, but we encapsulate it to make
     * sure we don't mix it up with other kinds of identifier that
     * we use.
     * @author Kees van Reeuwijk
     *
     */
    static final class WorkerIdentifier implements Serializable {
        private static final long serialVersionUID = 3271311796768467853L;
        final int value;

        WorkerIdentifier( int value )
        {
            this.value = value;
        }

        /**
         * Returns the hash code of this worker identifier.
         * @return A hash code for this identifier.
         */
        @Override
        public int hashCode() {
            return value;
        }

        /**
         * Returns true iff this worker identifier is equal to the given
         * one.
         * @param obj The object to compare to.
         * @return True iff the two identifiers are equal.
         */
        @Override
        public boolean equals( Object obj )
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final WorkerIdentifier other = (WorkerIdentifier) obj;
            if (value != other.value)
                return false;
            return true;
        }

        /** Returns a string representation of this worker.
         * 
         * @return The string representation.
         */
        @Override
        public String toString()
        {
            return "W" + value;
        }
    }

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
     * @param node The node this master belongs to.
     * @throws IOException Thrown if the master cannot be created.
     */
    Master( Ibis ibis, Node node ) throws IOException
    {
        super( "Master" );
        this.queue = new MasterQueue();
        this.node = node;
        sendPort = new PacketSendPort<MasterMessage>( ibis );
        receivePort = new PacketUpcallReceivePort<WorkerMessage>( ibis, Globals.masterReceivePortName, this );
        startTime = System.nanoTime();
    }

    /**
     * Start this master thread.
     */
    @Override
    public void start()
    {
        receivePort.enable();           // We're open for business.
        super.start();                  // Start the thread
    }

    /**
     * Set the local listener to the given class instance.
     * @param localListener The local listener to use.
     */
    void setLocalListener( PacketReceiveListener<MasterMessage> localListener )
    {
        sendPort.setLocalListener( localListener );
    }

    void setStopped()
    {
        synchronized( queue ) {
            stopped = true;
            queue.notifyAll();
        }
    }

    /**
     * Returns true iff this master is in stopped mode, has no
     * jobs in its queue, and has not outstanding jobs on its workers.
     * @return True iff this master has processed all jobs it ever will.
     */
    private boolean isFinished()
    {
        synchronized( queue ){
            if( !stopped ){
                return false;
            }
            if( !queue.isEmpty() ) {
                return false;
            }
            return workers.areIdle();
        }
    }

    private void unsubscribeWorker( WorkerIdentifier worker )
    {
        if( Settings.traceWorkerList ) {
            System.out.println( "unsubscribe of worker " + worker );
        }
        synchronized( queue ){
            workers.removeWorker( worker );
            queue.notifyAll();
        }
    }

    /**
     * A worker has sent use a status message for a job. Process it.
     * @param result The status message.
     */
    private void handleWorkerStatusMessage( JobCompletedMessage result )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received a worker status message " + result );
        }
        synchronized( queue ){
            workers.registerWorkerStatus( result );
            handledJobCount++;
            queue.notifyAll();
        }
    }

    private void handleResultMessage( TaskResultMessage m )
    {
        node.reportCompletion( m.task, m.result );
    }

    private void sendAcceptMessage( Master.WorkerIdentifier workerID )
    {
        ReceivePortIdentifier myport = receivePort.identifier();
        Worker.MasterIdentifier idOnWorker = workers.getMasterIdentifier( workerID );
        WorkerAcceptMessage msg = new WorkerAcceptMessage( idOnWorker, myport, workerID );

        long sz = sendPort.tryToSend( workerID.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( sz<0 ){
            synchronized( queue ) {
                workers.declareDead( workerID );
                queue.notifyAll();
            }
        }
    }

    /**
     * A worker has sent us a message asking for more work.
     * 
     * @param m The work request message.
     */
    private void handleWorkRequestMessage( WorkRequestMessage m )
    {
        WorkerIdentifier workerID = m.source;
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received work request message " + m + " from worker " + workerID );
        }
        synchronized( queue ){
            queue.incrementAllowance( workerID, workers );
            workers.registerCompletionInfo( workerID, m.completionInfo );
            queue.notifyAll();
        }
    }

    /**
     * A worker has sent us a message with its current task completion times.
     * 
     * @param m The update message.
     */
    private void handleWorkerUpdateMessage( WorkerUpdateMessage m )
    {
        WorkerIdentifier workerID = m.source;
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received worker update message " + m + " from worker " + workerID );
        }
        synchronized( queue ){
            workers.registerCompletionInfo( workerID, m.completionInfo );
            queue.notifyAll();
        }
    }

    /**
     * A worker has sent us a message to register itself with us. This is
     * just to tell us that it's out there. We tell it what our receive port is,
     * and which handle we have assigned to it, so that it can then inform us
     * of the types of jobs it supports.
     *
     * @param m The worker registration message.
     */
    private void handleRegisterWorkerMessage( RegisterWorkerMessage m )
    {
        WorkerIdentifier workerID;
        ReceivePortIdentifier worker = m.port;

        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master: received registration message " + m + " from worker " + worker );
        }
        if( m.supportedTypes.length == 0 ) {
            Globals.log.reportInternalError( "Worker " + worker + " has zero supported types??" );
        }
        synchronized( queue ) {
            boolean local = sendPort.isLocalListener( receivePort.identifier() );
            workerID = workers.subscribeWorker( receivePort.identifier(), worker, local, m.masterIdentifier, m.supportedTypes );
            sendPort.registerDestination( worker, workerID.value );
            workersToAccept.add( workerID );
            queue.notifyAll();
        }
    }

    /**
     * Returns true iff this listener is associated with the given port.
     * @param port The port it should be associated with.
     * @return True iff this listener is associated with the port.
     */
    @Override
    public boolean hasReceivePort( ReceivePortIdentifier port )
    {
        return port.equals( receivePort.identifier() );
    }

    /**
     * Handles message <code>msg</code> from worker.
     * @param msg The message we received.
     */
    @Override
    public void messageReceived( WorkerMessage msg )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master: received message " + msg );
        }
        if( msg instanceof JobCompletedMessage ) {
            JobCompletedMessage result = (JobCompletedMessage) msg;

            handleWorkerStatusMessage( result );
        }
        else if( msg instanceof TaskResultMessage ) {
            TaskResultMessage m = (TaskResultMessage) msg;

            handleResultMessage( m );
        }
        else if( msg instanceof WorkerUpdateMessage ) {
            WorkerUpdateMessage m = (WorkerUpdateMessage) msg;

            handleWorkerUpdateMessage( m );
        }
        else if( msg instanceof WorkRequestMessage ) {
            WorkRequestMessage m = (WorkRequestMessage) msg;

            handleWorkRequestMessage( m );
        }
        else if( msg instanceof RegisterWorkerMessage ) {
            RegisterWorkerMessage m = (RegisterWorkerMessage) msg;

            handleRegisterWorkerMessage( m );
        }
        else if( msg instanceof WorkerResignMessage ) {
            WorkerResignMessage m = (WorkerResignMessage) msg;

            unsubscribeWorker( m.source );
        }
        else {
            Globals.log.reportInternalError( "the master should handle message of type " + msg.getClass() );
        }
    }

    /**
     * Adds the given job to the work queue of this master.
     * @param job The job instance to add to the queue.
     */
    void submit( JobInstance job )
    {
        if( Settings.traceMasterProgress ) {
            System.out.println( "Master: received job " + job );
        }
        synchronized ( queue ) {
            incomingJobCount++;
            queue.submit( job );
            queue.notifyAll();
        }
    }

    /**
     * Adds the given job to the work queue of this master.
     * @param job The job instance to add to the queue.
     * @return The estimated time in ns it will take to complete the entire task
     *   instance this job instance belongs to.
     */
    long submitAndGetInfo( JobInstance job )
    {
        if( Settings.traceMasterProgress ) {
            System.out.println( "Master: received job " + job );
        }
        synchronized ( queue ) {
            incomingJobCount++;
            long queueTime = queue.submit( job );
            long res = queueTime + workers.getAverageCompletionTime( job.type );
            queue.notifyAll();
            return res;
        }
    }

    /**
     * @param worker The worker to send the job to.
     * @param job The job to send.
     */
    private void submitJobToWorker( Submission sub )
    {
        long jobId;

        synchronized( queue ){
            jobId = nextJobId++;
            sub.worker.registerJobStart( sub.job, jobId );
        }
        RunJobMessage msg = new RunJobMessage( sub.worker.identifierWithWorker, sub.worker.identifier, sub.job, jobId );
        long sz = sendPort.tryToSend( sub.worker.identifier.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( sz<0 ){
            // Try to put the paste back in the tube.
            synchronized( queue ){
                queue.submit( msg.job );
                sub.worker.retractJob( msg.jobId );
            }
        }
    }

    /** Keep submitting jobs until the queue is empty. We occasionally may
     * have to wait for workers to get ready.
     * FIXME: better comment, and perhaps better abstraction ordering.
     * 
     * @return True iff we want to keep running.
     */
    private boolean submitAllJobs()
    {
        boolean nowork;
        Submission sub = new Submission();
        WorkerIdentifier newWorker = null;

        boolean keepRunning = true;
        if( Settings.traceMasterProgress ){
            System.out.println( "Next round for master" );
        }

        while( true ) {
            synchronized( queue ){
                nowork = queue.selectSubmisson( sub, workers );
                if( nowork || sub.worker == null ){
                    if( !workersToAccept.isEmpty() ) {
                        // No workers or no work. We have  time to add a new worker.
                        newWorker = workersToAccept.removeFirst();
                    }
                    break;
                }
            }
            if( Settings.traceMasterQueue ) {
                System.out.println( "Selected " + sub.worker + " as best for job " + sub.job );
            }
            submitJobToWorker( sub );
        }
        // There are no jobs in the queue, or there are no workers ready.
        if( nowork && isFinished() ){
            // No jobs, and we are stopped; don't try to send new jobs.
            keepRunning = false;   // We're no longer busy.
        }
        else {
            if( Settings.traceMasterProgress ){
                System.out.println( "Master: nothing in the queue, or no ready workers; waiting" );
            }
            // Since the queue is empty, we can only wait for new jobs.
            if( newWorker != null ) {
                sendAcceptMessage( newWorker );
                newWorker = null;
            }
            try {
                synchronized( queue ){
                    if( !isFinished() ){
                        queue.wait();
                    }
                }
            } catch (InterruptedException e) {
                // Not interested.
            }
            keepRunning = true;
        }
        return keepRunning; // We're still busy.
    }

    /** Runs this master. */
    @Override
    public void run()
    {
        boolean active = true;  // Not yet stopped?

        if( Settings.traceMasterProgress ){
            System.out.println( "Starting master thread" );
        }
        while( active ){
            active = submitAllJobs( );
        }
        stopTime = System.nanoTime();
        System.out.println( "End of master thread" );
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    void removeIbis( IbisIdentifier theIbis )
    {
        workers.removeWorker( theIbis );
    }

    /** Returns the identifier of (the receive port of) this worker.
     * 
     * @return The identifier.
     */
    ReceivePortIdentifier identifier()
    {
        return receivePort.identifier();
    }

    /** Print some statistics about the entire master run. */
    void printStatistics( PrintStream s )
    {
        if( stopTime<startTime ) {
            System.err.println( "Worker didn't stop yet" );
        }
        queue.printStatistics( s );
        long workInterval = stopTime-startTime;
        s.printf( "Master: # workers          = %5d\n", workers.size() );
        s.printf( "Master: # inactive workers = %5d\n", workersToAccept.size() );
        s.printf( "Master: # incoming jobs    = %5d\n", incomingJobCount );
        s.printf( "Master: # handled jobs     = %5d\n", handledJobCount );
        s.println( "Master: run time           = " + Service.formatNanoseconds( workInterval ) );
        sendPort.printStats( s, "master send port" );
        workers.printStatistics( s );
    }

    CompletionInfo[] getCompletionInfo( TaskList tasks )
    {
        synchronized( queue ) {
            return queue.getCompletionInfo( tasks, workers );
        }
    }
}
