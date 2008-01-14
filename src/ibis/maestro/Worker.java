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
    private PacketBlockingReceivePort<MasterMessage> jobPort;
    private PacketSendPort<JobResult> resultPort;
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
        jobPort = new PacketBlockingReceivePort<MasterMessage>( ibis, "jobPort" );
        resultPort = new PacketSendPort<JobResult>( ibis );
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
                MasterMessage msg = jobPort.receive();
                if( msg instanceof MasterStoppedMessage ){
                    // FIXME: handle this.
                }
                else {
                    TaskMessage tm = (TaskMessage) msg;
                    Job job = tm.getJob();
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
                        JobReturn r = job.run();
                        System.err.println( "Job " + job + " completed; result: " + r );
                        resultPort.send( new JobResult( r, tm.getId() ), tm.getMaster() );
                    }
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
