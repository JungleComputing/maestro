package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;

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
    private final LinkedList<Job> queue = new LinkedList<Job>();
    private Hashtable<JobType,JobInfo> jobInfoTable = new Hashtable<JobType, JobInfo>();

    private CompletionListener completionListener;
    private boolean stopped = false;
    private long nextJobId = 0;
    private long incomingJobCount = 0;
    private long handledJobCount = 0;
    private long workerCount = 0;
    private final long startTime;
    private long stopTime = 0;

    private void unsubscribeWorker( ReceivePortIdentifier worker )
    {
	synchronized( workers ){
	    workers.removeWorker( worker );
	    if( Settings.traceWorkerList ) {
		System.out.println( "unsubscribe of worker " + worker );
	    }
	    workers.notifyAll();
	}
    }

    private void handleWorkerStatusMessage( WorkerStatusMessage result )
    {
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Received a worker status message " + result );
        }
        synchronized( workers ) {
            workers.registerWorkerStatus( receivePort.identifier(), result );
            workers.notifyAll();
        }
        synchronized( queue ){
            queue.notifyAll();
            handledJobCount++;
        }
    }

    private void handleJobResultMessage( JobResultMessage result )
    {
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Received a job result " + result );
        }
        synchronized( workers ) {
            workers.registerJobResult( result, completionListener );
            workers.notifyAll();
        }
        synchronized( queue ){
            queue.notifyAll();
            handledJobCount++;
        }
    }

    /**
     * A (presumably unregistered) worker has sent us a message asking for work.
     * 
     * @param m The message to handle.
     */
    private void handleWorkRequestMessage( WorkRequestMessage m )
    {
        ReceivePortIdentifier worker = m.source;
        ArrayList<JobType> allowedTypes = m.allowedTypes;
        long now = System.nanoTime();
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Received work request message " + m + " from worker " + worker + " at " + Service.formatNanoseconds(now) );
        }
        if( !workers.isKnownWorker( worker ) ){
            workers.subscribeWorker(receivePort.identifier(), worker );
        }
        workers.registerWorkerJobTypes( worker, allowedTypes );
        synchronized( queue ){
            for( Job e: queue ){
                if( allowedTypes.isEmpty() ){
                    // Apparantely, we've handled all types of job this worker knows about.
                    break;
                }
                JobType jobType = e.getType();

                int ix = allowedTypes.indexOf( jobType );
                if( ix>=0 ){
                    // We're in need of a worker for this type of job; increase the allowance
                    // of this worker.
                    workers.incrementAllowance(worker, jobType);

                    // Now remove it from the list to make sure other jobs of the same
                    // type don't trigger this again.
                    allowedTypes.remove( ix );
                }
            }
        }
        synchronized (workers ) {
            workers.notifyAll();            
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
        if( Settings.writeTrace ) {
            Globals.tracer.traceReceivedMessage( msg, p.identifier() );
        }
        if( msg instanceof WorkerStatusMessage ) {
            WorkerStatusMessage result = (WorkerStatusMessage) msg;

            handleWorkerStatusMessage( result );
        }
        else if( msg instanceof JobResultMessage ) {
            JobResultMessage result = (JobResultMessage) msg;

            handleJobResultMessage( result );
        }
        else if( msg instanceof WorkRequestMessage ) {
            WorkRequestMessage m = (WorkRequestMessage) msg;

            handleWorkRequestMessage( m );
        }
        else if( msg instanceof WorkerTimeSyncMessage ) {
            // Ignore this message, the fact that it was traced is enough.
        }
        else if( msg instanceof WorkerResignMessage ) {
            WorkerResignMessage m = (WorkerResignMessage) msg;

            unsubscribeWorker( m.source );
        }
        else {
            Globals.log.reportInternalError( "the master should handle message of type " + msg.getClass() );
        }
    }

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
     * @param l The completion listener to use.
     * @throws IOException Thrown if the master cannot be created.
     */
    public Master( Ibis ibis, CompletionListener l ) throws IOException
    {
        super( "Master" );
        completionListener = l;
        sendPort = new PacketSendPort<MasterMessage>( ibis );
        receivePort = new PacketUpcallReceivePort<WorkerMessage>( ibis, Globals.masterReceivePortName, this );
        receivePort.enable();		// We're open for business.
        startTime = System.nanoTime();    }

    synchronized void setStopped()
    {
        stopped = true;
        synchronized( queue ) {
            queue.notifyAll();
        }
        synchronized( workers ) {
            workers.notifyAll();
        }
    }

    private synchronized boolean isStopped()
    {
        return stopped;
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
        synchronized( queue ) {
            incomingJobCount++;
            queue.add( j );
            queue.notifyAll();
        }
        if( Settings.traceMasterProgress ) {
            System.out.println( "Master: received job " + j );
        }
    }

    /**
     * Registers a completion listener with this master.
     * @param l The completion listener to register.
     */
    public synchronized void setCompletionListener( CompletionListener l )
    {
        completionListener = l;
    }
    
    /**
     * Returns true iff this master is in stopped mode, has no
     * jobs in its queue, and has not outstanding jobs on its workers.
     * @return True iff this master has processed all jobs it ever will.
     */
    private boolean isFinished()
    {
        boolean workersIdle;
        boolean queueEmpty;

        if( !isStopped() ){
            return false;
        }
        synchronized( workers ){
            workersIdle = workers.areIdle();
            synchronized( queue ){
        	queueEmpty = queue.isEmpty();
            }
        }
        return workersIdle && queueEmpty;
    }

    private boolean waitForNewJobs()
    {
	// There are no jobs in the queue.
	if( isFinished() ){
	    // No jobs, and we are stopped; don't try to send new jobs.
	    return false;   // We're no longer busy.
	}
	if( Settings.traceMasterProgress ){
	    System.out.println( "Nothing in the queue; waiting" );
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
	return true;
    }

    private boolean submitJobs()
    {
	boolean res = true;
	if( Settings.traceMasterProgress ){
	    System.out.println( "Next round for master" );
	}
	submitWaitingJobs();
	res = waitForNewJobs();
	return res; // We're still busy.
    }

    /**
     * We know there are jobs in the queue. Try to submit as many as possible,
     * but make sure we don't over-fill the queue of the workers too much.
     * Once we submit them we cannot get them back.
     */
    private void submitWaitingJobs()
    {
        long now = System.nanoTime();
        
        while( true ) {
            // Try to find a job for each worker, best worker first.
            int jobToRun = -1;
            WorkerInfo worker = null;

            synchronized( queue ) {
                // This is a pretty big operation to do in one atomic
                // 'gulp'. TODO: see if we can break it up somehow.
                int ix = queue.size();
                while( ix>0 ) {
                    ix--;

                    Job job = queue.get( ix );
                    JobType jobType = job.getType();
                    worker = workers.selectBestWorker( jobType );
                    if( worker != null ) {
                        // We have a job that we can run.
                        jobToRun = ix;
                        break;
                    }
                }
            }
            if( worker == null ){
                break;
            }
            Job job;
            long jobId;

            synchronized( queue ) {
                job = queue.remove( jobToRun );
                jobId = nextJobId++;
            }
            JobType jobType = job.getType();
            JobInfo jobInfo = jobInfoTable.get( jobType );
            if( jobInfo == null ) {
                jobInfo = new JobInfo( jobType );
                jobInfoTable.put( jobType, jobInfo );
            }
            if( Settings.traceFastestWorker ) {
                System.out.println( "Selected worker " + worker + " as best for job " + job );
            }
            RunJobMessage msg = new RunJobMessage( job, receivePort.identifier(), jobId );
            synchronized( workers ) {
                worker.registerJobStart( job, jobInfo, jobId, now );
            }
            try {
                if( Settings.writeTrace ) {
                    Globals.tracer.traceSentMessage( msg, worker.getPort() );
                }
                long len = sendPort.send( msg, worker.getPort() );
                jobInfo.updateSendSize( len );
            } catch (IOException e) {
                // Try to put the paste back in the tube.
                synchronized( queue ){
                    queue.add( job );
                }
                worker.retractJob( jobId );
                Globals.log.reportError( "Could not send job to " + worker + ": putting toothpaste back in the tube" );
                e.printStackTrace();
                // We don't try to roll back job start time, since the worker
                // may in fact be busy.
            }
        }
    }

    /** Runs this master. */
    @Override
    public void run()
    {
	boolean busy = true;

	if( Settings.traceMasterProgress ){
	    System.out.println( "Starting master thread" );
	}
	while( busy ){
	    busy = submitJobs( );
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
        // FIXME: reschedule any outstanding jobs on this ibis.	    
        workers.removeWorkers( theIbis );
    }

    /**
     * A new ibis has joined the computation.
     * @param theIbis The ibis that has joined.
     */
    public void addIbis( IbisIdentifier theIbis )
    {
        // FIXME: implement this.
    }

    /** Returns the identifier of (the receive port of) this worker.
     * 
     * @return The identifier.
     */
    public ReceivePortIdentifier identifier()
    {
        return receivePort.identifier();
    }

    /** Given the identifier for a worker, wait until this worker
     * has subscribed itself. Due to possibly unreliable communication,
     * a remote worker may never subscribe itself. Therefore, only
     * this method is only safe for local workers.
     * @param identifier The worker.
     */
    void waitForSubscription(ReceivePortIdentifier identifier)
    {
        while( true ) {
            synchronized( workers ) {
                if( workers.contains( identifier ) ) {
                    return;
                }
                try {
                    workers.wait();
                } catch (InterruptedException e) {
                    // Ignore.
                }
            }
        }
    }

    /**
     * Wait until the work queue is empty.
     * 
     * Used to make sure we drain the work queue as efficiently as possible
     * before we shut down the master.
     */
    public void waitForEmptyQueue() {
        while( true ) {
            synchronized( queue ) {
                if( queue.isEmpty() ) {
                    return;
                }
                try {
                    queue.wait();
                } catch (InterruptedException e) {
                    // Ignore.
                }
            }
        }
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
    }

    @Override
    public void reportResult(Job job, JobProgressValue value) {
	System.err.println( "FIXME: implement reportResult() job=" + job + " value=" + value );
    }

}
