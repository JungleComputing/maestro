package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;

/**
 * A master in the Maestro flow graph framework.
 * 
 * @author Kees van Reeuwijk
 * 
 */
@SuppressWarnings("synthetic-access")
public class Master extends Thread implements PacketReceiveListener<WorkerMessage>, JobContext
{
    private final WorkerList workers = new WorkerList();
    private final PacketUpcallReceivePort<WorkerMessage> receivePort;
    private final PacketSendPort<MasterMessage> sendPort;
    private final AbstractList<Job> queue = new ArrayList<Job>();

    private boolean stopped = false;
    private long nextJobId = 0;
    private long incomingJobCount = 0;
    private long handledJobCount = 0;
    private int workerCount = 0;
    private final long startTime;
    private long stopTime = 0;

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
     * @throws IOException Thrown if the master cannot be created.
     */
    public Master( Ibis ibis ) throws IOException
    {
        super( "Master" );
        sendPort = new PacketSendPort<MasterMessage>( ibis );
        receivePort = new PacketUpcallReceivePort<WorkerMessage>( ibis, Globals.masterReceivePortName, this );
        receivePort.enable();		// We're open for business.
        startTime = System.nanoTime();
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

    private void unsubscribeWorker( int worker )
    {
	if( Settings.traceWorkerList ) {
	    System.out.println( "unsubscribe of worker " + worker );
	}
	synchronized( queue ){
	    workers.removeWorker( worker );
	    queue.notifyAll();
	}
    }

    private void handleWorkerStatusMessage( WorkerStatusMessage result )
    {
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Received a worker status message " + result );
        }
        synchronized( queue ){
            workers.registerWorkerStatus( receivePort.identifier(), result );
            handledJobCount++;
            queue.notifyAll();
        }
    }
    
    private void sendAcceptMessage( int workerID, ReceivePortIdentifier myport, int idOnWorker )
    {
        WorkerAcceptMessage msg = new WorkerAcceptMessage( idOnWorker, myport );
        try{
            sendPort.send( msg, workerID, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        }
        catch( IOException e ){
            synchronized( queue ) {
        	workers.declareDead( workerID );
            }
        }
    }

    /**
     * A worker has sent us a message asking for work. This may be a new
     * worker, or a known one who wants more work.
     * 
     * @param m The work request message.
     */
    private void handleWorkRequestMessage( WorkRequestMessage m )
    {
	int workerID = m.source;
	if( Settings.traceWorkerProgress ){
	    Globals.log.reportProgress( "Received work request message " + m + " from worker " + workerID );
	}
	synchronized( queue ) {
	    // We already know that this worker can handle this type of
	    // job, but if he asks again, we can give a larger allowance.
	    // We only do this if at the moment there is a job of this
	    // type in the queue.
	    for( Job e: queue ){
		JobType jobType = e.getType();

		// We're in need of a worker for this type of job; try to 
		// increase the allowance of this worker.
		if( workers.incrementAllowance( workerID, jobType ) ) {
		    queue.notifyAll();
		    break;
		}
	    }
	}
    }


    /**
     * A worker has sent us a message to register itself with us. This is
     * just to tell us that it's out there and can handle jobs of the given
     * type. We give each worker an allowance of 1 job; it it wants more it
     * will have to ask for it.
     *
     * @param m The worker registration message.
     */
    private void handleRegisterWorkerMessage( RegisterWorkerMessage m )
    {
        boolean sendAcceptMessage = false;
        int workerID = -1;
        ReceivePortIdentifier worker = m.port;
        JobType allowedType = m.allowedType;
        if( Settings.traceMasterProgress ){
            Globals.log.reportProgress( "Master: received registration message " + m + " from worker " + worker );
        }
        synchronized( queue ) {
            if( !workers.isKnownWorker( worker ) ){
        	workerID = workerCount++;
        	workers.subscribeWorker( receivePort.identifier(), worker, workerID, m.masterIdentifier );
        	sendAcceptMessage = true;
            }
            workers.registerWorkerJobTypes( worker, allowedType );
        }
        if( sendAcceptMessage ) {
            sendPort.registerDestination( worker, workerID );
            sendAcceptMessage( workerID, receivePort.identifier(), m.masterIdentifier );
        }
    }

    /**
     * Handles message <code>msg</code> from worker.
     * @param p The port this was received on.
     * @param msg The message we received.
     */
    @Override
    public void messageReceived( PacketUpcallReceivePort<WorkerMessage> p, WorkerMessage msg )
    {
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Master: received message " + msg );
        }
        if( msg instanceof WorkerStatusMessage ) {
            WorkerStatusMessage result = (WorkerStatusMessage) msg;

            handleWorkerStatusMessage( result );
        }
        else if( msg instanceof RegisterWorkerMessage ) {
            RegisterWorkerMessage m = (RegisterWorkerMessage) msg;

            handleRegisterWorkerMessage( m );
        }
        else if( msg instanceof WorkRequestMessage ) {
            WorkRequestMessage m = (WorkRequestMessage) msg;

            handleWorkRequestMessage( m );
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
     * @param submitter The job that submitted this job.
     * @param j The job to add to the queue.
     */
    public void submit( Job submitter, Job j )
    {
        if( Settings.traceMasterProgress ) {
            System.out.println( "Master: received job " + j );
        }
        synchronized( queue ) {
            incomingJobCount++;
            queue.add( j );
            queue.notifyAll();
        }
    }

    /** Keep submitting jobs until the queue is empty. We occasionally may
     * have to wait for workers to get ready.
     * 
     * @return True iff we want to keep running.
     */
    private boolean submitAllJobs()
    {
	RunJobMessage msg;

	boolean keepRunning = true;
	if( Settings.traceMasterProgress ){
	    System.out.println( "Next round for master" );
	}
	long now = System.nanoTime();
	boolean noWork;

	while( true ) {
	    // Try to find a job for each worker, best worker first.
	    int jobToRun = -1;
	    WorkerInfo worker = null;
	
	    // Try to get some work handed out. We can't just get the first job
	    // from the queue and hand it out, since there may not be
	    // a ready worker for that type of job, while there are for the
	    // type of job of a subsequent job.
	    //
	    // FIXME: however, once we've tried to place one instance of
	    // a job type, it is no use trying to place other instances
	    // of that same job type. What we now do is potentially
	    // very expensive.
	    synchronized( queue ) {
		Job job;
		long jobId;
		
	        // This is a pretty big operation to do in one atomic
	        // 'gulp'. TODO: see if we can break it up somehow.
	        int ix = queue.size();
	        noWork = (ix == 0);
	        while( ix>0 ) {
	            ix--;
	
	            job = queue.get( ix );
	            JobType jobType = job.getType();
	            worker = workers.selectBestWorker( jobType );
	            if( worker != null ) {
	                // We have a job that we can run.
	                jobToRun = ix;
	                break;
	            }
	        }
	        if( worker == null ){
	            break;
	        }
	        // We have a job and a worker. Submit the job.
	        job = queue.remove( jobToRun );
	        jobId = nextJobId++;
	        worker.registerJobStart( job, jobId, now );
		    msg = new RunJobMessage( worker.identifier, job, worker.identifierWithWorker, jobId );
	    }
	    if( Settings.traceFastestWorker ) {
	        System.out.println( "Selected worker " + worker + " as best for job " + msg.job );
	    }
	    // FIXME: do we really have to send worker.identifier? isn't jobId enough?
	    try {
	        sendPort.send( msg, worker.identifier, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
	    }
	    catch (IOException e) {
	        // Try to put the paste back in the tube.
	        synchronized( queue ){
	            queue.add( msg.job );
	        }
	        worker.retractJob( msg.jobId );
	        Globals.log.reportError( "Could not send job to " + worker + ": putting toothpaste back in the tube" );
	        e.printStackTrace();
	        // We don't try to roll back job start time, since the worker
	        // may in fact be busy.
	    }
	}
	if( noWork ) {
	    workers.reduceAllowances();
	}
	// There are no jobs in the queue, or there are no workers ready.
	if( noWork && isFinished() ){
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
        try {
            receivePort.close();
        }
        catch( IOException x ) {
            // Nothing we can do about it.
        }
        stopTime = System.nanoTime();
        System.out.println( "End of master thread" );
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis( IbisIdentifier theIbis )
    {
        workers.removeWorker( theIbis );
    }

    /** Returns the identifier of (the receive port of) this worker.
     * 
     * @return The identifier.
     */
    public ReceivePortIdentifier identifier()
    {
        return receivePort.identifier();
    }

    /** Print some statistics about the entire master run. */
    public void printStatistics()
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
    }

    /**
     * Report the 
     * @param receiver
     * @param value
     */
    @Override
    public void reportResult( ReportReceiver receiver, JobProgressValue value )
    {
	IbisIdentifier ibis = receiver.getIbis();

	JobResultMessage msg = new JobResultMessage( -1, value, receiver.getId() );
        try {
            sendPort.send( msg, ibis, Globals.workerReceivePortName, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        }
        catch (IOException e) {
            e.printStackTrace( System.err );
        }
    }

}
