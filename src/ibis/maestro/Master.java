package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.io.Serializable;

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

    private boolean stopped = false;
    private long nextJobId = 0;
    private long incomingJobCount = 0;
    private long handledJobCount = 0;
    private int workerCount = 0;
    private final long startTime;
    private long stopTime = 0;

    static final class WorkerIdentifier implements Serializable {
        private static final long serialVersionUID = 3271311796768467853L;
        final int value;

        WorkerIdentifier( int value )
	{
	    this.value = value;
	}

	/**
	 * @return A hash code for this identifier.
	 */
	@Override
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + value;
	    return result;
	}

	/**
	 * @param obj The object to compare to.
	 * @return True iff the two identifiers are equal.
	 */
	@Override
	public boolean equals(Object obj) {
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
     * @param typeInformation The type information class to use.
     * @throws IOException Thrown if the master cannot be created.
     */
    Master( Ibis ibis, Node node, TypeInformation typeInformation ) throws IOException
    {
        super( "Master" );
        this.queue = new MasterQueue( typeInformation );
        this.node = node;
        sendPort = new PacketSendPort<MasterMessage>( ibis );
        receivePort = new PacketUpcallReceivePort<WorkerMessage>( ibis, Globals.masterReceivePortName, this );
        receivePort.enable();		// We're open for business.
        startTime = System.nanoTime();
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
    private void handleWorkerStatusMessage( WorkerStatusMessage result )
    {
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received a worker status message " + result );
        }
        synchronized( queue ){
            workers.registerWorkerStatus( receivePort.identifier(), result );
            handledJobCount++;
            queue.notifyAll();
        }
    }

    private void handleResultMessage( ResultMessage m )
    {
	node.reportCompletion( m.id, m.result );
    }

    private void sendAcceptMessage( Master.WorkerIdentifier workerID, ReceivePortIdentifier myport, Worker.MasterIdentifier idOnWorker )
    {
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
     * A worker has sent us a message telling us it can handle the given
     * job types. Presumably some of the types are new.
     * Tell our own worker about these types; it may allow it to support
     * new types.
     * 
     * @param m The type registration message.
     */
    private void handleRegisterTypeMessage( RegisterTypeMessage m )
    {
        WorkerIdentifier workerID = m.source;
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Received work request message " + m + " from worker " + workerID );
        }
        JobType allowedTypes[] = m.allowedType;
        synchronized( queue ) {
            workers.updateJobTypes( workerID, allowedTypes );
            queue.notifyAll();
        }
        node.updateNeighborJobTypes( allowedTypes );
    }

    /**
     * A worker has sent us a message asking for work. This may be a new
     * worker, or a known one who wants more work.
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
            queue.notifyAll();
        }
    }

    /**
     * A worker has sent us a message to register itself with us. This is
     * just to tell us that it's out there. We tell it what our receive port
     * and the handle we assigned to it are, so that it can then inform us
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
        synchronized( queue ) {
            workerID = workers.subscribeWorker( receivePort.identifier(), worker, m.masterIdentifier );
            sendPort.registerDestination( worker, workerID.value );
            workerCount++;
        }
        sendAcceptMessage( workerID, receivePort.identifier(), m.masterIdentifier );
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
        if( msg instanceof WorkerStatusMessage ) {
            WorkerStatusMessage result = (WorkerStatusMessage) msg;

            handleWorkerStatusMessage( result );
        }
        else if( msg instanceof ResultMessage ) {
            ResultMessage m = (ResultMessage) msg;
            
            handleResultMessage( m );
        }
        else if( msg instanceof WorkRequestMessage ) {
            WorkRequestMessage m = (WorkRequestMessage) msg;

            handleWorkRequestMessage( m );
        }
        else if( msg instanceof RegisterTypeMessage ) {
            RegisterTypeMessage m = (RegisterTypeMessage) msg;

            handleRegisterTypeMessage( m );
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
     * Note that the master uses a priority queue for its scheduling,
     * so jobs may not be executed in chronological order.
     * @param j The job to add to the queue.
     * @param id The identifier of the task this job belongs to.
     */
    void submit( Job j, TaskIdentifier id )
    {
        if( Settings.traceMasterProgress ) {
            System.out.println( "Master: received job " + j );
        }
        synchronized ( queue ) {
            incomingJobCount++;
            queue.submit( j, id );
            queue.notifyAll();
        }
    }

    /**
     * Adds the given job to the work queue of this master.
     * Note that the master uses a priority queue for its scheduling,
     * so jobs may not be executed in chronological order.
     * @param j The job to add to the queue.
     * @param id The identifier of the task this job belongs to.
     */
    void submitWhenRoom( Job j, TaskIdentifier id )
    {
        if( Settings.traceMasterProgress ) {
            System.out.println( "Master: received job " + j );
        }
        int maxQueueLength = workers.getWorkerCount()*Settings.JOBS_PER_WORKER;
        synchronized ( queue ) {
            // First wait for the queue to drain to a reasonable size.
            while( queue.size()>maxQueueLength ) {
        	try {
		    queue.wait();
		} catch (InterruptedException e) {
		    // Not interesting.
		}
            }
            incomingJobCount++;
            queue.submit( j, id );
            queue.notifyAll();
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
        RunJobMessage msg = new RunJobMessage( sub.worker.identifierWithWorker, sub.worker.identifier, sub.job, jobId, sub.taskId );
        long sz = sendPort.tryToSend( sub.worker.identifier.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( sz<0 ){
            // Try to put the paste back in the tube.
            synchronized( queue ){
        	queue.submit( msg.job, sub.taskId );
        	sub.worker.retractJob( msg.jobId );
            }
        }
    }

    /** Keep submitting jobs until the queue is empty. We occasionally may
     * have to wait for workers to get ready.
     * 
     * @return True iff we want to keep running.
     */
    private boolean submitAllJobs()
    {
	boolean nowork;
        Submission sub = new Submission();

	boolean keepRunning = true;
	if( Settings.traceMasterProgress ){
	    System.out.println( "Next round for master" );
	}

	while( true ) {
            synchronized( queue ){
                nowork = queue.selectJob( sub, workers );
                if( nowork || sub.worker == null ){
                    break;
                }
            }
	    if( Settings.traceFastestWorker ) {
	        System.out.println( "Selected worker " + sub.worker + " as best for job " + sub.job );
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
		System.out.println( "Master: nothing in the queue; waiting" );
	    }
	    // Since the queue is empty, we can only wait for new jobs.
	    try {
		// There is nothing to do; Wait for new queue entries.
		if( Settings.traceMasterProgress ){
		    System.out.println( "Master: waiting for new jobs in queue" );
		}
		synchronized( queue ){
		    queue.wait();
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
    void printStatistics()
    {
        if( stopTime<startTime ) {
            System.err.println( "Worker didn't stop yet" );
        }
        long workInterval = stopTime-startTime;
        System.out.printf( "Master: # workers        = %5d\n", workerCount );
        System.out.printf( "Master: # incoming jobs  = %5d\n", incomingJobCount );
        System.out.printf( "Master: # handled jobs   = %5d\n", handledJobCount );
        System.out.println( "Master: run time         = " + Service.formatNanoseconds( workInterval ) );
        sendPort.printStats( "master send port" );
        workers.printStatistics( System.out );
    }
}
