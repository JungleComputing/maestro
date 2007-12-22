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
    private PacketBlockingReceivePort<JobMessage> jobPort;
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
        jobRequestPort.send( new JobRequest( jobPort.identifier() ), master, "requestPort" );
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
        jobPort = new PacketBlockingReceivePort<JobMessage>( ibis, "jobPort" );
        resultPort = new PacketSendPort<JobResult<R>>( ibis );
        jobRequestPort = new PacketSendPort<JobRequest>( ibis );
        jobPort.enable();
    }

    /** Runs this worker. */
    public void run()
    {
        setStopped( false );
        while( !isStopped() ) {
            System.err.println( "Next round for worker" );
            try {
                sendWorkRequest();
                JobMessage msg = jobPort.receive();
                System.err.println( "Received job message " + msg );
                if( msg instanceof RunJobMessage ) {
                    RunJobMessage<R> rjm = (RunJobMessage<R>) msg;
                    Job<R> job = rjm.getJob();
                    R r = job.run();
                    System.err.println( "Job " + job + " completed; result: " + r );
                    resultPort.send( new JobResult<R>( r, rjm.getId() ), rjm.getResultPort() );
                }
                else if( msg instanceof MasterStoppedMessage ) {
                    // FIXME: register the fact that this master has stopped.
                }
                else if( msg instanceof NoJobMessage ) {
                    try {
                        // FIXME: more sophistication.
                        Thread.sleep( BACKOFF_DELAY );
                    }
                    catch( InterruptedException e ) {
                        // Somebody woke us, but we don't care.
                    }
                }
                else {
                    System.err.println( "Unknown job message type " + msg.getClass() );
                }
            }
            catch( ClassNotFoundException x ){
        	x.printStackTrace();
        	setStopped( true );
            }
            catch( IOException x ){
        	x.printStackTrace();
        	setStopped( true );
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
