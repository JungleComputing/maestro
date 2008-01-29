package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.LinkedList;
import java.util.PriorityQueue;

/**
 * A master in the Maestro flow graph framework.
 * 
 * @author Kees van Reeuwijk
 * 
 */
@SuppressWarnings("synthetic-access")
public class Master extends Thread  implements PacketReceiveListener<WorkerMessage> {
    private final WorkerList workers = new WorkerList();
    private final PacketUpcallReceivePort<WorkerMessage> receivePort;
    private final PacketSendPort<MasterMessage> sendPort;
    private final PriorityQueue<Job> queue = new PriorityQueue<Job>();
    private final LinkedList<PingTarget> pingTargets = new LinkedList<PingTarget>();
    private CompletionListener completionListener;
    private boolean stopped = false;

    private void unsubscribeWorker( ReceivePortIdentifier worker )
    {
        synchronized( workers ){
            workers.unsubscribeWorker( worker );
            workers.notifyAll();
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
        PingTarget t = null;
        long receiveTime = System.nanoTime();
        long benchmarkTime = m.getBenchmarkTime();
        int workThreads = m.workThreads;

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
            synchronized( workers ){
                workers.subscribeWorker( worker, workThreads, pingTime-benchmarkTime, m.getBenchmarkScore() );
                System.out.println( "A new worker " + worker + " has arrived" );
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
                Globals.tracer.traceSentMessage( msg, worker );
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

    /**
     * Handles message <code>msg</code> from worker.
     * @param p The port this was received on.
     * @param msg The message we received.
     */
    @Override
    public void packetReceived( PacketUpcallReceivePort<WorkerMessage> p, WorkerMessage msg )
    {
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Master: received message " + msg );
        }
        if( Settings.traceNodes ) {
            Globals.tracer.traceReceivedMessage( msg, p.identifier() );
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
        receivePort = new PacketUpcallReceivePort<WorkerMessage>( ibis, Globals.masterReceivePortName, this );
        receivePort.enable();
    }

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
     * @param j The job to add to the queue.
     */
    public void submit( Job j )
    {
        synchronized( queue ) {
            queue.add( j );
            queue.notifyAll();
        }
        System.out.println( "Submitted job " + j + " to master" );
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
                        System.out.println( "Master: waiting for new jobs in queue" );
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
                    System.out.println( "No ready workers; waiting" );
                    // FIXME: advertise for new workers.
                    long sleepTime = workers.getBusyInterval();
                    try {
                        // There is nothing to do; Wait for workers to complete
                        // or new workers.
                        synchronized( workers ){
                            System.out.println( "Master: waiting for a ready worker" );
                            workers.wait( sleepTime/1000000 );
                        }
                    } catch (InterruptedException e) {
                        // Not interested.
                    }
                }
                else {
                    // We have a worker willing to the job, now get a job to do.
                    System.out.println( "Submitting job to worker" );
                    Job job;
                    synchronized( queue ) {
                        job = queue.remove();
                    }
                    RunJobMessage msg = new RunJobMessage( job, receivePort.identifier() );
                    synchronized( workers ) {
                        // FIXME: compute proper completion time.
                        long completionTime = 0L;
                        long arrivalTime = 0L;
                	worker.registerJobStart( job, msg.id, completionTime, arrivalTime );
                    }
                    try {
                        sendPort.send( msg, worker.getPort() );
                        if( Settings.traceNodes ) {
                            Globals.tracer.traceSentMessage( msg, worker.getPort() );
                        }
                    } catch (IOException e) {
                        // Try to put the paste back in the tube.
                        synchronized( queue ){
                            queue.add( job );
                        }
                        worker.retractJob( msg.id );
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
        System.out.println( "Master is stopping. Queue drained, waiting for outstanding jobs" );
        while( true ){
            synchronized( workers ){
                if( workers.areIdle() ){
                    // No outstanding jobs, we're done.
                    break;
                }
                try {
                    workers.wait();
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
        System.out.println( "End of master thread" );
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis(IbisIdentifier theIbis)
    {
        workers.removeIbis( theIbis );
        // FIXME: reschedule any outstanding jobs on this ibis.
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
    public void waitForSubscription(ReceivePortIdentifier identifier)
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
}
