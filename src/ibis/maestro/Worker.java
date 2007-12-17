package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;

/**
 * A worker in the Maestro master-worker system.
 * @author Kees van Reeuwijk
 * @param <R> The result type of the jobs that this worker runs.
 *
 */
@SuppressWarnings("synthetic-access")
public class Worker<R> implements Runnable {
    private PacketBlockingReceivePort<JobQueueEntry<R>> jobPort;
    private PacketSendPort<JobResult<R>> resultPort;
    private PacketSendPort<JobRequest> jobRequestPort;
    private static final long BACKOFF_DELAY = 10;  // In ms.
    private boolean stopped;
    private final IbisIdentifier master;

    private synchronized void setStopped( boolean val ) {
	stopped = val;
    }
    
    private synchronized boolean isStopped() {
	return stopped;
    }

    private void sendWorkRequest() throws IOException
    {
        System.err.println( "Asking for work" );
        jobRequestPort.send( new JobRequest( jobPort.identifier() ), master, "reqestPort" );
    }

    /**
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to
     * @param server The (request port of) the master.
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis, IbisIdentifier server ) throws IOException
    {
	this.master = server;
        jobPort = new PacketBlockingReceivePort<JobQueueEntry<R>>( ibis, "jobPort" );
        resultPort = new PacketSendPort<JobResult<R>>( ibis );
        jobRequestPort = new PacketSendPort<JobRequest>( ibis );
    }

    /** Runs this worker. */
    public void run()
    {
        setStopped( false );
        while( !isStopped() ) {
            System.err.println( "Next round for worker" );
            try {
                sendWorkRequest();
                JobQueueEntry<R> msg = jobPort.receive();
                Job<R> job = msg.getJob();
                System.err.println( "Received job " + job );
                if( job == null ) {
                    try {
                        // FIXME: more sophistication.
                        Thread.sleep( BACKOFF_DELAY );
                    }
                    catch( InterruptedException e ) {
                        // Somebody woke us, but we don't care.
                    }
                }
                else {
                    R r = job.run();
                    System.err.println( "Job " + job + " completed; result: " + r );
                    resultPort.send( new JobResult<R>( r, msg.getId() ), msg.getMaster() );
                }
            }
            catch( ClassNotFoundException x ){
        	x.printStackTrace();
            }
            catch( IOException x ){
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
