package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * A worker in the Maestro multiple master-worker system.
 * @author Kees van Reeuwijk
 */
@SuppressWarnings("synthetic-access")
public class Worker extends Thread {
    private final PacketUpcallReceivePort<MasterMessage> receivePort;
    private final PacketSendPort<WorkerMessage> sendPort;
    private final PriorityBlockingQueue<RunJobMessage> jobQueue = new PriorityBlockingQueue<RunJobMessage>();
    private final LinkedList<IbisIdentifier> unusedNeighbors = new LinkedList<IbisIdentifier>();
    private final Master localMaster;
    private boolean stopped;
    private ReceivePortIdentifier exclusiveMaster = null;

    /**
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to.
     * @param master The master that jobs may submit new jobs to.
     * @param serial The serial number of this worker on this node.
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis, Master master, int serial ) throws IOException
    {
        super( "Worker" );
        setDaemon(false);
	this.localMaster = master;
        receivePort = new PacketUpcallReceivePort<MasterMessage>( ibis, Globals.workerReceivePortName + serial, new MessageHandler() );
        receivePort.enable();
        sendPort = new PacketSendPort<WorkerMessage>( ibis );
        synchronized( unusedNeighbors ){
            // Add yourself to the list of neighbors.
            unusedNeighbors.add( ibis.identifier() );
        }
    }
    
    ReceivePortIdentifier identifier()
    {
	return receivePort.identifier();
    }

    private synchronized void setStopped( boolean val ) {
	stopped = val;
    }
    
    private synchronized boolean isStopped() {
	return stopped;
    }

    /**
     * Returns the identifier of the job submission port of this worker.
     * @return The port identifier.
     */
    public ReceivePortIdentifier getReceivePort()
    {
        return receivePort.identifier();
    }

    private IbisIdentifier getUnusedNeighbor()
    {
        IbisIdentifier res;
        synchronized( unusedNeighbors ){
            if( unusedNeighbors.isEmpty() ){
                res = null;
            }
            else {
                res = unusedNeighbors.getFirst();
            }
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

    private class MessageHandler implements PacketReceiveListener<MasterMessage> {
        /**
         * Handles job request message <code>msg</code>.
         * @param p The port on which the packet was received.
         * @param msg The job we received and will put in the queue.
         */
        public void packetReceived(PacketUpcallReceivePort<MasterMessage> p, MasterMessage msg) {
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Recieved a message " + msg );
            }
            if( Settings.traceNodes ) {
        	Globals.tracer.traceReceivedMessage( msg );
            }
            if( msg instanceof RunJobMessage ){
                RunJobMessage runJobMessage = (RunJobMessage) msg;

                handleRunJobMessage(runJobMessage);
            }
            else if( msg instanceof AddNeighborsMessage ){
                AddNeighborsMessage addMsg = (AddNeighborsMessage) msg;

                handleAddNeighborsMessage(addMsg);
            }
            else if( msg instanceof PingMessage ){
                PingMessage ping = (PingMessage) msg;

                handlePingMessage(ping);
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

        /**
         * Handle a message containing new neighbors.
         * 
         * @param msg The message to handle.
         */
        private void handleAddNeighborsMessage(AddNeighborsMessage msg) {
            addNeighbors( msg.getNeighbors() );
        }

        private void sendResignMessage( ReceivePortIdentifier master ) throws IOException
        {
            WorkerResignMessage msg = new WorkerResignMessage( receivePort.identifier() );
            sendPort.send(msg, master);
            if( Settings.traceNodes ) {
                Globals.tracer.traceSentMessage(msg);
            }
        }

        /**
         * Handle a message containing a new job to run.
         * 
         * @param msg The message to handle.
         */
        private void handleRunJobMessage(RunJobMessage msg) {
            msg.setStartTime( System.nanoTime() );
            jobQueue.add( msg );
        }

        /**
         * @param msg The message to handle.
         */
        private void handlePingMessage(PingMessage msg) {
            long startTime = System.nanoTime();

            double benchmarkScore = msg.benchmarkResult();
            long benchmarkTime = System.nanoTime()-startTime;
            ReceivePortIdentifier master = msg.getMaster();
            PingReplyMessage m = new PingReplyMessage( receivePort.identifier(), benchmarkScore, benchmarkTime );
            try {
                sendPort.send(m, master);
                if( Settings.traceNodes ) {
                    Globals.tracer.traceSentMessage(m);
                }
            }
            catch( IOException x ){
                Globals.log.reportError( "Cannot send ping reply to master " + master );
                x.printStackTrace( Globals.log.getPrintStream() );
            }
        }
    }

    private void findNewMaster()
    {
        IbisIdentifier m = getUnusedNeighbor();
        if( m == null ){
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "No neighbors to ask for work" );
            }
            return;
        }
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Asking neighbor " + m + " for work" );
        }
        try {
            WorkRequestMessage msg = new WorkRequestMessage( receivePort.identifier() );
	    sendPort.send( msg, m, Globals.masterReceivePortName );
	    if( Settings.traceNodes ) {
		Globals.tracer.traceSentMessage(msg);
	    }
        }
        catch( IOException x ){
            Globals.log.reportError( "Failed to send a work request message to neighbor " + m );
            x.printStackTrace();
        }
    }

    /**
     * Returns true iff there are jobs in the queue.
     * @return Are there jobs in the queue?
     */
    private boolean areWaitingJobs()
    {
	boolean res;

	synchronized( jobQueue ) {
	    res = !jobQueue.isEmpty();
	}
	return res;
    }

    /** Runs this worker. */
    @Override
    public void run()
    {
        System.out.println( "Starting worker thread" );
        setStopped( false );
        while( true ) {
            if( Settings.traceWorkerProgress ){
        	System.out.println( "Next round for worker" );
            }
            synchronized( jobQueue ) {
        	if( !areWaitingJobs() ) {
        	    if( isStopped() ) {
        		break;
        	    }
        	    try {
        		findNewMaster();
        		// There is nothing to do; Wait for new queue entries.
        		synchronized( jobQueue ){
        		    jobQueue.wait();
        		}
        	    } catch (InterruptedException e) {
        		// Not interested.
        	    }
        	}
        	else {
        	    try {
        		RunJobMessage jobMessage = jobQueue.take();
        		if( jobMessage != null ){
        		    Job job = jobMessage.getJob();
        		    if( Settings.traceWorkerProgress ){
        			System.out.println( "Starting job " + job );
        		    }
        		    JobReturn r = job.run( localMaster );
        		    long computeTime = System.nanoTime()-jobMessage.getStartTime();
        		    if( Settings.traceWorkerProgress ){
        			System.out.println( "Job " + job + " completed in " + computeTime + "ns; result: " + r );
        		    }
        		    try {
        			JobResultMessage msg = new JobResultMessage( receivePort.identifier(), r, jobMessage.getId(), computeTime );
        			sendPort.send( msg, jobMessage.getResultPort() );
        			if( Settings.traceNodes ) {
        			    Globals.tracer.traceSentMessage( msg );
        			}
        		    }
        		    catch( IOException x ){
        			// Something went wrong in sending the result back.
        			Globals.log.reportError( "Worker failed to send job result" );
        			x.printStackTrace( Globals.log.getPrintStream() );
        		    }
        		}
        	    }
        	    catch( InterruptedException x ){
        		if( Settings.traceWorkerProgress ){
        		    System.out.println( "Worker take() got interrupted: " + x );
        		}
        		// We got interrupted while waiting for the next job. Just ignore it.
        	    }

        	}
            }
        }
        System.out.println( "Ended worker thread" );
    }
    
    /**
     * Stop this worker.
     */
    public void stopWorker()
    {
	setStopped( true );
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis(IbisIdentifier theIbis) {
	synchronized( unusedNeighbors ) {
	    unusedNeighbors.remove( theIbis );
	}
	// FIXME: remove any jobs from this ibis from our queue.
    }

    /**
     * A new ibis has joined the computation.
     * @param theIbis The ibis that has joined.
     */
    public void addIbis(IbisIdentifier theIbis) {
        synchronized( unusedNeighbors ){
            unusedNeighbors.add( theIbis );
        }
    }
    
    /** Quickly do as much as possible to prevent new work from reaching us. */
    public void closeDown() {
	try {
	    receivePort.close();
	}
	catch( IOException x ) {
	    // Nothing we can do about it. Ignore.
	}
    }

    /**
     * Shut down this worker after all the jobs currently in the queue have been processed.
     * That is, both the work queue and the list of outstanding jobs should be empty.
     * This method returns after the master has been shut down.
     */
    public void finish() {
	// First wait for job queue to drain.
	while( true ) {
	    synchronized( jobQueue ) {
		if(jobQueue.isEmpty() ) {
		    break;
		}
		try {
		    jobQueue.wait();
		} catch (InterruptedException e) {
		    // Not interesting.
		}
	    }
	}
	// FIXME: wait for last job to finish.
	setStopped( true );	
    }

    /**
     * Send resign messages to all masters except for the one given here.
     * @param identifier the master we shouldn't resign from.
     */
    public void resignExcept(ReceivePortIdentifier identifier)
    {
        exclusiveMaster = identifier;
    }
}
