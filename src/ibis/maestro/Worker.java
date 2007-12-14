package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;

/**
 * A worker in the Maestro master-worker system.
 * @author Kees van Reeuwijk
 *
 */
@SuppressWarnings("synthetic-access")
public class Worker<R> implements Runnable {
    private PacketBlockingReceivePort<JobQueueEntry<R>> jobPort;
    private PacketSendPort<JobResult<R>> resultPort;
    private PacketSendPort<JobRequest> jobRequestPort;
    private static final long BACKOFF_DELAY = 10;  // In ms.
    private boolean stopped;

    private synchronized void setStopped( boolean val ) {
	stopped = val;
    }
    
    private synchronized boolean isStopped() {
	return stopped;
    }

    private void sendWorkRequest( ReceivePortIdentifier receiver ) throws IOException
    {
        jobRequestPort.send( new JobRequest( jobPort.identifier() ), receiver );
    }

    /**
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis ) throws IOException
    {
        jobPort = new PacketBlockingReceivePort<JobQueueEntry<R>>( ibis, "jobPort" );
        resultPort = new PacketSendPort<JobResult<R>>( ibis, "resultPort" );
        jobRequestPort = new PacketSendPort<JobRequest>( ibis, "requestPort" );
    }

    /** Runs this worker.
     */
    public void run()
    {
        setStopped( false );
        while( !isStopped() ) {
            try {
                JobQueueEntry<R> msg = jobPort.receive();
                Job<R> job = msg.getJob();
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
                    resultPort.send( new JobResult<R>( r, msg.getId() ), msg.getMaster() );
                }
            }
            catch( ClassNotFoundException x ){

            }
            catch( IOException x ){

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
