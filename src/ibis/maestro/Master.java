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

    /** Targets of outstanding ping messages. */
    private final LinkedList<PingTarget> pingTargets = new LinkedList<PingTarget>();
    private CompletionListener completionListener;
    private boolean stopped = false;
    private long nextJobNo = 0;
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
            workers.registerJobResult( receivePort.identifier(), result, completionListener );
            workers.notifyAll();
        }
        synchronized( queue ){
            queue.notifyAll();
            handledJobCount++;
        }
    }

    /**
     * A new worker has sent a reply to our ping message.
     * The reply contains the performance of the worker on a benchmark,
     * and the time it took to complete the benchmark (the two are
     * not necessarily related).
     * From the round-trip time of our ping request we compute communication
     * overhead. Together all this information gives us a reasonable guess
     * for the performance of the new worker relative to our other workers.
     *
     * @param m The message to handle.
     */
    private void handlePingReplyMessage( PingReplyMessage m )
    {
        final long receiveTime = System.nanoTime();
        long pingTime = 0L;
        ArrayList<JobType> allowedTypes;

        // First, search for the worker in our list of
        // outstanding pings.
        ReceivePortIdentifier worker = m.source;
        synchronized( pingTargets ){
            PingTarget t = null;

            for( PingTarget w: pingTargets ){
                if( w.hasIdentifier( worker ) ){
                    t = w;
                }
            }
            if( t == null ){
                Globals.log.reportInternalError( "Worker " + worker + " replied to a ping that wasn't sent: ignoring" );
                return;
            }
            pingTime = receiveTime-t.getSendTime();
            allowedTypes = t.allowedTypes;
            pingTargets.remove( t );
        }
        synchronized( workers ){
            workerCount++;
            workers.subscribeWorker( receivePort.identifier(), worker, allowedTypes );
            System.out.println( "A new worker " + worker + " has arrived" );
            workers.notifyAll();
        }
        synchronized( queue ) {
            queue.notifyAll();
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
        long now = System.nanoTime();
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Received work request message " + m + " from worker " + worker + " at " + Service.formatNanoseconds(now) );
        }
        PingTarget t = new PingTarget( worker, now, m.allowedTypes );
        synchronized( pingTargets ){
            pingTargets.add( t );
        }
        if( Settings.writeTrace ) {
            // Send a message to the new worker to synchronize clocks.
            // This message is only sent if we are tracing, because only the
            // tracer is interested.
            MasterTimeSyncMessage syncmsg = new MasterTimeSyncMessage( receivePort.identifier() );
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Sending time sync message " + syncmsg + " to worker " + worker );
            }            
            try {
                Globals.tracer.traceSentMessage( syncmsg, worker );
                sendPort.send( syncmsg, worker );
            }
            catch( IOException x ){
                synchronized( pingTargets ){
                    pingTargets.remove( t );
                }
                Globals.log.reportError( "Cannot send ping message to worker " + worker );
                x.printStackTrace( Globals.log.getPrintStream() );
            }
        }
        PingMessage msg = new PingMessage( receivePort.identifier() );
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Sending ping message " + msg + " to worker " + worker );
        }            
        try {
            if( Settings.writeTrace ) {
                Globals.tracer.traceSentMessage( msg, worker );
            }
            sendPort.send( msg, worker );
        }
        catch( IOException x ){
            synchronized( pingTargets ){
                pingTargets.remove( t );
            }
            Globals.log.reportError( "Cannot send ping message to worker " + worker );
            x.printStackTrace( Globals.log.getPrintStream() );
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
        else if( msg instanceof PingReplyMessage ) {
            PingReplyMessage m = (PingReplyMessage) msg;

            handlePingReplyMessage( m );
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
     * Returns true iff there are jobs in the queue.
     * @return Are there jobs in the queue?
     */
    private boolean areWaitingJobs()
    {
        boolean res;

        synchronized( queue ) {
            res = !queue.isEmpty();
        }
        return res;
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
	    synchronized( queue ){
		if( Settings.traceMasterProgress ){
		    System.out.println( "Master: waiting for new jobs in queue" );
		}
		queue.wait();
	    }
	} catch (InterruptedException e) {
	    // Not interested.
	}
	return true;
    }

    private boolean submitJobs( WorkerSelector sel )
    {
	boolean res = true;
	if( Settings.traceMasterProgress ){
	    System.out.println( "Next round for master" );
	}
	if( areWaitingJobs() ) {
	    submitWaitingJobs( sel );
	}
	else {
	    res = waitForNewJobs();
	}
	return res; // We're still busy.
    }

    /**
     * Given a list of workers and a job type, return the index in workers of the
     * one with the fastest submission frequency.
     * @param readyWorkers The list of workers.
     * @param jobType The job type.
     * @return The index in <code>workers</code> of the fastest worker for this job type, or
     *         -1 if none of the workers can handle this job.
     */
    private int selectBestWorker( ArrayList<WorkerInfo> readyWorkers, JobType jobType ) {
        int best = -1;
        long bestInterval = Long.MAX_VALUE;

        System.out.println( "Selecting best of " + readyWorkers.size() + " workers for job of type " + jobType );
        for( int ix=0; ix<readyWorkers.size(); ix++ ) {
            WorkerInfo wi = readyWorkers.get( ix );
            long val = wi.getSubmissionInterval( jobType );

            if( val<bestInterval ) {
                bestInterval = val;
                best = ix;
            }
        }
        System.out.println( "Selected worker " + best + " for job of type " + jobType );
        return best;
    }

    /**
     * We know there are jobs in the queue. Try to submit as many as possible,
     * but make sure we don't over-fill the queue of the workers too much.
     * Once we submit them we cannot get them back.
     * @param sel The worker administration class to use. Passed in to
     *            prevent creating a new one for every iteration.
     */
    private void submitWaitingJobs( WorkerSelector sel )
    {
        long now = System.nanoTime();
        ArrayList<WorkerInfo> readyWorkers = workers.getReadyWorkers( now );
        
        while( readyWorkers.size()>0 ) {
            // Try to find a job for each worker, best worker first.
            int jobToRun = -1;
            int workerToRun = -1;

            synchronized( queue ) {
                // This is a pretty big operation to do in one atomic
                // 'gulp'. TODO: see if we can break it up somehow.
                int ix = queue.size();
                while( ix>0 ) {
                    ix--;
                    
                    Job job = queue.get( ix );
                    JobType jobType = job.getType();
                    int worker = selectBestWorker( readyWorkers, jobType );
                    if( worker>=0 ) {
                        // We have a job that we can run.
                        workerToRun = worker;
                        jobToRun = ix;
                        break;
                    }
                }
            }
            if( jobToRun>=0 ) {
                Job job;
                long jobId;
                WorkerInfo worker = readyWorkers.remove( workerToRun );

                synchronized( queue ) {
                    job = queue.remove( jobToRun );
                    jobId = nextJobNo++;
                }
                JobType jobType = job.getType();
                JobInfo jobInfo = jobInfoTable.get( jobType );
                if( jobInfo == null ) {
                    jobInfo = new JobInfo( jobType );
                    jobInfoTable.put( jobType, jobInfo );
                }
                if( Settings.traceFastestWorker ) {
                    System.out.println( "Selected worker " + worker + " as best node for job " + job );
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
        long sleepInterval = workers.getNextSubmissionTime()-now;
        long sleepMS = (sleepInterval+500000)/1000000;

        if( sleepMS>0 ) {
            try {
                // There is nothing to do; Wait for workers to complete
                // or new workers.
                synchronized( workers ){
                    if( Settings.traceMasterProgress ){
                        System.out.println( "Master: waiting " + Service.formatNanoseconds( sleepInterval ) + " for a ready worker" );
                    }
                    workers.wait( sleepMS );
                }
            } catch (InterruptedException e) {
                // Not interested.
            }        		
        }
        else {
            if( Settings.traceMasterProgress ){
                System.out.println( "It is useless to go to sleep for " + Service.formatNanoseconds( sleepInterval ) );
            }
        }
    }

    /** Runs this master. */
    @Override
    public void run()
    {
	WorkerSelector sel = new WorkerSelector();
	boolean busy = true;

	if( Settings.traceMasterProgress ){
	    System.out.println( "Starting master thread" );
	}
	while( busy ){
	    busy = submitJobs( sel );
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
