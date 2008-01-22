package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.LinkedList;
import java.util.PriorityQueue;

/**
 * A send queue in the Maestro flow graph framework.
 * 
 * @author Kees van Reeuwijk
 * 
 */
@SuppressWarnings("synthetic-access")
public class Master extends Thread {
    private final WorkerList workers = new WorkerList();
    private final PacketUpcallReceivePort<WorkerMessage> receivePort;
    private final PacketSendPort<MasterMessage> sendPort;
    private final PriorityQueue<Job> queue = new PriorityQueue<Job>();
    private final LinkedList<ActiveJob> activeJobs = new LinkedList<ActiveJob>();
    private final LinkedList<PingTarget> pingTargets = new LinkedList<PingTarget>();
    private CompletionListener completionListener;
    private long jobno = 0;
    private boolean stopped = false;

    private void unsubscribeWorker( ReceivePortIdentifier worker )
    {
	workers.unsubscribeWorker( worker );
    }

    private class MessageHandler implements PacketReceiveListener<WorkerMessage> {
        /**
         * Handles job request message <code>message</code>.
         * @param result The job request message.
         */
        @Override
        public void packetReceived(PacketUpcallReceivePort<WorkerMessage> p, WorkerMessage msg) {
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Received message " + msg );
            }
            if( msg instanceof JobResultMessage ) {
        	JobResultMessage result = (JobResultMessage) msg;

        	handleJobResultMessage(result);
            }
            else if( msg instanceof WorkRequestMessage ) {
        	WorkRequestMessage m = (WorkRequestMessage) msg;

        	handleWorkRequestMessage( m );
            }
            else if( msg instanceof PingReplyMessage ) {
        	PingReplyMessage m = (PingReplyMessage) msg;

        	handlePingReplyMessage(m);
            }
            else if( msg instanceof WorkerResignMessage ) {
        	WorkerResignMessage m = (WorkerResignMessage) msg;

        	unsubscribeWorker( m.getPort() );
            }
            else {
        	Globals.log.reportInternalError( "the master should handle message of type " + msg.getClass() );
            }
        }

        /**
         * A worker has sent us the result of a job. Register this information.
         * 
         * @param result The message to handle.
         */
        private void handleJobResultMessage( JobResultMessage result )
        {
            long id = result.getId();    // The identifier of the job, as handed out by us.

            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Received a job result " + result );
            }
            ActiveJob e = searchQueueEntry( id );
            if( e == null ) {
                Globals.log.reportInternalError( "ignoring reported result from job with unknown id " + id );
                return;
            }
            if( completionListener != null ) {
        	completionListener.jobCompleted( e.getJob(), result.getResult() );
            }
            WorkerInfo worker = e.getWorker();
            long now = System.nanoTime();
            worker.registerJobCompletionTime( now, result.getComputeTime() );
            synchronized( activeJobs ) {
                activeJobs.remove( e );
                this.notify();   // Wake up master thread; we might have stopped.
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
        private void handlePingReplyMessage(PingReplyMessage m) {
            PingTarget t = null;
            long receiveTime = System.nanoTime();
            long benchmarkTime = m.getBenchmarkTime();

            // First, search for the worker in our list of
            // outstanding pings.
            ReceivePortIdentifier worker = m.getWorker();
            synchronized( pingTargets ){
                for( PingTarget w: pingTargets ){
                    if( w.hasIdentifier( worker ) ){
                        t = w;
                        break;
                    }
                }
            }
            if( t == null ){
                Globals.log.reportInternalError( "Worker " + worker + " replied to a ping that wasn't sent: ignoring" );
            }
            else {
                long pingTime = t.getSendTime()-receiveTime;
                synchronized( pingTargets ){
                    pingTargets.remove( t );
                }
                workers.subscribeWorker( worker, pingTime-benchmarkTime, m.getBenchmarkScore() );
            }
        }

        /**
         * A (presumably unregistered) worker has sent us a message asking for work.
         * 
         * @param m The message to handle.
         */
        private void handleWorkRequestMessage(WorkRequestMessage m) {
            ReceivePortIdentifier worker = m.getPort();
            long now = System.nanoTime();
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Received work request message " + m + " from worker " + worker + " at " + Service.formatNanoseconds(now) );
            }
            PingTarget t = new PingTarget( worker, now );
            synchronized( pingTargets ){
                pingTargets.add( t );
            }
            PingMessage msg = new PingMessage( receivePort.identifier() );
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Sending ping message " + msg + " to worker " + worker );
            }            
            try {
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
    }

    /** Creates a new master instance.
     * @param ibis The Ibis instance this master belongs to.
     * @param l The completion listener to use.
     * @throws IOException Thrown if the master cannot be created.
     */
    public Master( Ibis ibis, CompletionListener l ) throws IOException
    {
        completionListener = l;
        sendPort = new PacketSendPort<MasterMessage>( ibis );
        receivePort = new PacketUpcallReceivePort<WorkerMessage>( ibis, Globals.masterReceivePortName, new MessageHandler() );
        receivePort.enable();
    }

    /**
     * Given a job identifier, returns the job queue entry with that id, or null.
     * @param id The job identifier to search for.
     * @return The JobQueueEntry of the job with this id, or null if there isn't one.
     */
    private ActiveJob searchQueueEntry( long id )
    {
        setDaemon(false);
        // Note that we blindly assume that there is only one entry with
        // the given id. Reasonable because we hand out the ids ourselves...
        synchronized( activeJobs ) {
            for( ActiveJob e: activeJobs ) {
                if( e.getId() == id ) {
                    return e;
                }
            }
        }
        return null;
    }

    private synchronized void setStopped( boolean val ) {
	stopped = val;
	this.notify();
    }

    private synchronized boolean isStopped()
    {
	return stopped;
    }

    /**
     * Adds the given job to the work queue of this master.
     * Note that the master uses a priority queue for its scheduling,
     * so jobs may not be executed in chronological order.
     * @param j The job to add to the queue.
     */
    public void submit( Job j ){
        synchronized( queue ) {
            queue.add( j );
        }
        startJobs();   // Try to start some jobs.
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
     * Returns true iff there are active jobs.
     * @return True iff there are active jobs.
     */
    private boolean areActiveJobs()
    {
        boolean res;

        synchronized( activeJobs ){
            res = !activeJobs.isEmpty();
        }
        return res;
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
     * As long as there are jobs in the queue and ready workers, send jobs to
     * those workers.
     */
    private void startJobs()
    {
        long id;
	while( areWaitingJobs() ) {
	    WorkerInfo worker = workers.getFastestWorker();
	    if( worker == null ) {
		// FIXME: advertise for new workers.
		return;
	    }
	    Job job;
	    synchronized( queue ) {
		job = queue.remove();
	    }
            synchronized( this ){
                id = jobno++;
            }
	    long startTime = System.nanoTime();
	    RunJobMessage msg = new RunJobMessage( job, id, receivePort.identifier() );
	    ActiveJob j = new ActiveJob( job, id, startTime, worker );
	    synchronized( activeJobs ) {
		activeJobs.add( j );
	    }
	    worker.registerJobStartTime( startTime );
	    try {
		sendPort.send( msg, worker.getPort() );
	    } catch (IOException e) {
                synchronized( queue ){
                    queue.add( job );
                }
                synchronized( activeJobs ){
                    activeJobs.remove( j );
                }
                Globals.log.reportError( "Could not send job to " + worker + ": put toothpaste back in the tube" );
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
        System.out.println( "Starting master thread" );
	// We simply wait until we have reached the stop state.
	// and there are no outstanding jobs.
	while( !isStopped() && !areActiveJobs() ) {
            startJobs();
	    try {
                long sleepTime = workers.getBusyInterval();
                Thread.sleep( sleepTime );
	    }
	    catch( InterruptedException x ) {
		// Nothing to do.
	    }
	}
	try {
	    receivePort.close();
	}
	catch( IOException x ) {
	    // Nothing we can do about it.
	}
        System.out.println( "Ending master thread" );
    }
    
    /** Stops  this master.   */
    public void stopQueue()
    {
	setStopped( true );
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis(IbisIdentifier theIbis) {
	// FIXME: implement this.
    }

    /**
     * A new ibis has joined the computation.
     * @param theIbis The ibis that has joined.
     */
    public void addIbis(IbisIdentifier theIbis) {
	// FIXME: implement this.
    }
}
