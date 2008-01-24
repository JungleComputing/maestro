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
    private boolean stopped = false;

    private void unsubscribeWorker( ReceivePortIdentifier worker )
    {
	workers.unsubscribeWorker( worker );
        synchronized( workers ){
            workers.notifyAll();
        }
    }

    private class MessageHandler implements PacketReceiveListener<WorkerMessage> {
        /**
         * Handles job request message <code>message</code>.
         * @param result The job request message.
         */
        @Override
        public void packetReceived( PacketUpcallReceivePort<WorkerMessage> p, WorkerMessage msg )
        {
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Received message " + msg );
            }
            if( Settings.traceNodes ) {
        	Globals.tracer.traceReceivedMessage( msg );
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
            long id = result.jobid;    // The identifier of the job, as handed out by us.

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
            if( isStopped() ) {
        	// If we're stopped, just ignore the message.
        	return;
            }
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
                synchronized( workers ){
                    workers.notifyAll();
                }
            }
        }

        /**
         * A (presumably unregistered) worker has sent us a message asking for work.
         * 
         * @param m The message to handle.
         */
        private void handleWorkRequestMessage( WorkRequestMessage m )
        {
            if( isStopped() ) {
        	// If we're stopped, just ignore the message.
        	return;
            }
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
                if( Settings.traceNodes ) {
                    Globals.tracer.traceSentMessage( msg );
                }
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
        super( "Master" );
        setDaemon(false);
        
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

    synchronized void setStopped()
    {
	stopped = true;
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
            queue.notifyAll();
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

    /** Runs this master. */
    @Override
    public void run()
    {
        System.out.println( "Starting master thread" );
        while( true ){
            System.out.println( "Next round for master" );
            if( !areWaitingJobs() ) {
                if( isStopped() ){
                    // No jobs, and we are stopped; don't try to send new jobs.
                    break;
                }
                System.out.println( "Nothing in the queue; waiting" );
                // Since the queue is empty, we can only wait for new jobs.
                try {
                    // There is nothing to do; Wait for new queue entries.
                    synchronized( queue ){
                        queue.wait();
                    }
                } catch (InterruptedException e) {
                    // Not interested.
                }
            }
            else {
                // We have at least one job, now try to give it to a worker.
                WorkerInfo worker = workers.getFastestWorker();
                if( worker == null ) {
                    // FIXME: advertise for new workers.
                    long sleepTime = workers.getBusyInterval();
                    try {
                        // There is nothing to do; Wait for workers to complete
                        // or new workers.
                        synchronized( workers ){
                            workers.wait( sleepTime/1000000 );
                        }
                    } catch (InterruptedException e) {
                        // Not interested.
                    }
                }
                else {
                    // We have a worker willing to the job, now get a job to do.
                    Job job;
                    synchronized( queue ) {
                        job = queue.remove();
                    }
                    long startTime = System.nanoTime();
                    RunJobMessage msg = new RunJobMessage( job, receivePort.identifier() );
                    ActiveJob j = new ActiveJob( job, msg.id, startTime, worker );
                    synchronized( activeJobs ) {
                        activeJobs.add( j );
                    }
                    worker.registerJobStartTime( startTime );
                    try {
                        sendPort.send( msg, worker.getPort() );
                        if( Settings.traceNodes ) {
                            Globals.tracer.traceSentMessage( msg );
                        }
                    } catch (IOException e) {
                        // Try to put the paste back in the tube.
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
        }
        // At this point we are shutting down, but we have to wait for outstanding jobs to
        // complete.
        while( true ){
            synchronized( activeJobs ){
                if( activeJobs.isEmpty() ){
                    // No outstanding jobs, we're done.
                    break;
                }
                try {
                    // Wait for a change in the active jobs status.
                    activeJobs.wait();
                }
                catch( InterruptedException x ){
                    // Ignore.
                }
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
    
    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis(IbisIdentifier theIbis) {
        workers.removeIbis( theIbis );
        // FIXME: reschedule any outstanding jobs on this ibis.
    }

    /**
     * A new ibis has joined the computation.
     * @param theIbis The ibis that has joined.
     */
    public void addIbis(IbisIdentifier theIbis) {
	// FIXME: implement this.
    }

    /**
     * Gracefully shut down this master after all the jobs currently in the queue have been processed.
     * That is, both the work queue and the list of outstanding jobs should be empty.
     * This method returns after the master has been shut down.
     */
    public void finish() {
	setStopped();
        System.err.println( "Waiting for master queue to drain" );
	boolean busy = true;
	while( busy ) {
	    synchronized( queue ) {
		try {
		    queue.wait();
		} catch (InterruptedException e) {
		    // Not interesting.
		}
		busy = !queue.isEmpty();
	    }
	}
        System.err.println( "Waiting for active jobs to terminate" );
	busy = true;
	while( busy ) {
	    synchronized( activeJobs ) {
	        try {
	            activeJobs.wait();
	        } catch (InterruptedException e) {
	            // Not interesting.
	        }
		busy = !activeJobs.isEmpty();
	    }
	}
        System.err.println( "Master has finished" );
	try {
	    // First make sure no new workers try to reach us.
	    // Unfortunately, we can only now close the receive
	    // port, because our workers need to report their
	    // results.
	    receivePort.close();
	}
	catch( IOException x ) {
	    // Nothing we can do about it.
	}
    }

    /** Returns the identifier of (the receive port of) this worker.
     * 
     * @return The identifier.
     */
    public ReceivePortIdentifier identifier()
    {
        return receivePort.identifier();
    }
}
