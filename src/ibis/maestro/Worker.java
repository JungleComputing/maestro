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
    private boolean stopped;

    /**
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis ) throws IOException
    {
        receivePort = new PacketUpcallReceivePort<MasterMessage>( ibis, "jobPort", new MessageHandler() );
        sendPort = new PacketSendPort<WorkerMessage>( ibis );
        synchronized( unusedNeighbors ){
            // Add yourself to the list of neighbors.
            unusedNeighbors.add( ibis.identifier() );
        }
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
        }

        /**
         * Handle a message containing new neighbors.
         * 
         * @param msg The message to handle.
         */
        private void handleAddNeighborsMessage(AddNeighborsMessage msg) {
            addNeighbors( msg.getNeighbors() );
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
            sendPort.send( new WorkRequestMessage( receivePort.identifier() ), m, "requestPort" );
        }
        catch( IOException x ){
            Globals.log.reportError( "Failed to send a work request message to neighbor " + m );
            x.printStackTrace();
        }
    }

    /** Runs this worker. */
    @Override
    public void run()
    {
        setStopped( false );
        while( !isStopped() ) {
            if( Settings.traceWorkerProgress ){
                System.out.println( "Next round for worker" );
            }
            try {
                if( jobQueue.isEmpty() ){
                    findNewMaster();
                }
                RunJobMessage tm = jobQueue.take();
                if( tm != null ){
                    Job job = tm.getJob();
                    if( Settings.traceWorkerProgress ){
                        System.out.println( "Starting job " + job );
                    }
                    JobReturn r = job.run();
                    long computeTime = System.nanoTime()-tm.getStartTime();
                    if( Settings.traceWorkerProgress ){
                        System.out.println( "Job " + job + " completed in " + computeTime + "ns; result: " + r );
                    }
                    sendPort.send( new JobResultMessage( r, tm.getId(), computeTime ), tm.getResultPort() );
                }
            }
            catch( InterruptedException x ){
                if( Settings.traceWorkerProgress ){
                    System.out.println( "Worker take() got interrupted: " + x );
                }
                // We got interrupted while waiting for the next job. Just ignore it.
            }
            catch( IOException x ){
                // Something went wrong in sending the result back.
                Globals.log.reportError( "Worker failed to send job result" );
                x.printStackTrace( Globals.log.getPrintStream() );
            }
        }
    }
    
    /**
     * Stop this worker.
     */
    public void stopWorker()
    {
	setStopped( true );
    }
}
