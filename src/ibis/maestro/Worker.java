package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * A worker in the Maestro master-worker system.
 * @author Kees van Reeuwijk
 */
@SuppressWarnings("synthetic-access")
public class Worker implements Runnable {
    private PacketUpcallReceivePort<MasterMessage> jobPort;
    private PacketSendPort<WorkerMessage> resultPort;
    private final PriorityBlockingQueue<RunJobMessage> jobQueue = new PriorityBlockingQueue<RunJobMessage>();
    private final LinkedList<IbisIdentifier> unusedNeighbors = new LinkedList<IbisIdentifier>();
    private boolean stopped;

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
    public ReceivePortIdentifier getJobPort()
    {
        return jobPort.identifier();
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
        for( IbisIdentifier n: l ){
            synchronized( unusedNeighbors ){
                unusedNeighbors.add( n );
            }
        }
    }

    private class JobEnqueueHandler implements PacketReceiveListener<MasterMessage> {
        /**
         * Handles job request message <code>request</code>.
         * @param p The port on which the packet was received.
         * @param job The job we received and will put in the queue.
         */
        public void packetReceived(PacketUpcallReceivePort<MasterMessage> p, MasterMessage job) {
            System.err.println( "Recieved a job " + job );
            if( job instanceof RunJobMessage ){
                jobQueue.add( (RunJobMessage) job );                
            }
            else if( job instanceof AddNeighborsMessage ){
                addNeighbors( ((AddNeighborsMessage) job).getNeighbors() );
            }
            else {
                System.err.println( "FIXME: handle " + job );
            }
        }
    }

    /**
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis ) throws IOException
    {
        jobPort = new PacketUpcallReceivePort<MasterMessage>( ibis, "jobPort", new JobEnqueueHandler() );
        resultPort = new PacketSendPort<WorkerMessage>( ibis );
    }
    
    private void findNewMaster()
    {
        IbisIdentifier m = getUnusedNeighbor();
        if( Settings.traceWorkerProgress ){
            System.err.println( "Asking for work" );
        }
        try {
            resultPort.send( new WorkRequestMessage( jobPort.identifier() ), m, "requestPort" );
        }
        catch( IOException x ){
            System.err.println( "Failed to send a registration message to master " + m );
            x.printStackTrace();
        }
    }

    /** Runs this worker. */
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
                    long startTime = System.nanoTime();
                    JobReturn r = job.run();
                    long computeTime = System.nanoTime()-startTime;
                    if( Settings.traceWorkerProgress ){
                        System.out.println( "Job " + job + " completed in " + computeTime + "ns; result: " + r );
                    }
                    resultPort.send( new JobResultMessage( r, tm.getId() ), tm.getResultPort() );
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
                System.err.println( "Worker failed to send job result" );
                x.printStackTrace();
            }
        }
    }
    
    /**
     * Stop this worker.
     */
    public void stop()
    {
	setStopped( true );
    }
}
