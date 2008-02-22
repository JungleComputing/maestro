package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

/**
 * A worker in the Maestro multiple master-worker system.
 * @author Kees van Reeuwijk
 */
@SuppressWarnings("synthetic-access")
public class Worker extends Thread implements WorkSource, PacketReceiveListener<MasterMessage> {
    private final ArrayList<JobType> allowedTypes;
    private final PacketUpcallReceivePort<MasterMessage> receivePort;
    private final PacketSendPort<WorkerMessage> sendPort;
    private CompletionListener completionListener;
    private long queueEmptyMoment = 0L;
    private final LinkedList<RunJobMessage> queue = new LinkedList<RunJobMessage>();
    private final ArrayList<IbisIdentifier> jobSources = new ArrayList<IbisIdentifier>();
    private static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
    private final WorkThread workThreads[] = new WorkThread[numberOfProcessors];
    private boolean stopped = false;
    private final long startTime;
    private long stopTime;
    private long idleTime = 0;      // Cumulative idle time during the run.
    private long queueTime = 0;     // Cumulative queue time of all jobs.
    private int jobCount = 0;
    private long workTime = 0;
    private int runningJobs = 0;
    private final Random rng = new Random();

    /**
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to.
     * @param master The master that jobs may submit new jobs to.
     * @param allowedTypes The types of job this worker can handle.
     * @param completionListener The listener for job completion reports.
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis, Master master, ArrayList<JobType> allowedTypes, CompletionListener completionListener ) throws IOException
    {
        super( "Worker" );   // Create a thread with a name.
        this.allowedTypes = allowedTypes;
        this.completionListener = completionListener;
        receivePort = new PacketUpcallReceivePort<MasterMessage>( ibis, Globals.workerReceivePortName, this );
        sendPort = new PacketSendPort<WorkerMessage>( ibis );
        for( int i=0; i<numberOfProcessors; i++ ) {
            WorkThread t = new WorkThread( this, master );
            workThreads[i] = t;
            t.start();
        }
        if( Settings.writeTrace ){
            Globals.tracer.traceAlias( master.identifier(), receivePort.identifier() );
        }
        receivePort.enable();   // We're open for business.
        startTime = System.nanoTime();
    }

    private synchronized boolean isStopped()
    {
        return stopped;
    }

    /**
     * Returns the identifier of the job submission port of this worker.
     * @return The port identifier.
     */
    public ReceivePortIdentifier identifier()
    {
        return receivePort.identifier();
    }

    /** Removes and returns a random job source from the list of
     * known job sources. Returns null if the list is empty.
     * 
     * @return The job source, or null if there isn't one.
     */
    private IbisIdentifier getRandomJobSource()
    {
        IbisIdentifier res;

        synchronized( jobSources ){
            final int size = jobSources.size();
            if( size == 0 ){
                return null;
            }
            int ix = rng.nextInt( size );
            res = jobSources.remove( ix );
        }
        return res;
    }

    private void addJobSources( IbisIdentifier l[] )
    {
        synchronized( jobSources ){
            for( IbisIdentifier n: l ){
                if( !jobSources.contains(n) ){
                    jobSources.add( n );
                }
            }
        }
        // This is a good reason to wake up the queue.
        synchronized (queue ) {
            queue.notifyAll();
        }
    }

    /**
     * A new ibis has joined the computation.
     * @param theIbis The ibis that has joined.
     */
    void addJobSource( IbisIdentifier theIbis )
    {
        synchronized( jobSources ){
            if( !jobSources.contains(theIbis) ){
                jobSources.add( theIbis );
            }
        }
        // This is a good reason to wake up the queue.
        synchronized (queue ) {
            queue.notifyAll();
        }
    }

    private void findNewMaster()
    {
        IbisIdentifier m = getRandomJobSource();
        if( m == null ){
            return;
        }
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Asking neighbor " + m + " for work" );
        }
        try {
            WorkRequestMessage msg = new WorkRequestMessage( receivePort.identifier(), allowedTypes );
    
            if( Settings.writeTrace ) {
                // FIXME: compute a receive port identifier for this one.
                Globals.tracer.traceSentMessage( msg, null );
            }
            sendPort.send( msg, m, Globals.masterReceivePortName );
        }
        catch( IOException x ){
            Globals.log.reportError( "Failed to send a work request message to neighbor " + m );
            x.printStackTrace();
        }
    }

    /**
     * Handle a message containing new neighbors.
     * 
     * @param msg The message to handle.
     */
    private void handleAddNeighborsMessage(AddNeighborsMessage msg)
    {
        addJobSources( msg.getNeighbors() );
    }

    /**
     * Handle a message containing a new job to run.
     * 
     * @param msg The message to handle.
     */
    private void handleRunJobMessage( RunJobMessage msg )
    {
        long now = System.nanoTime();
	msg.setQueueTime( now );
        synchronized( queue ) {
            long queueEmptyInterval = 0L;
            if( queueEmptyMoment>0 ){
        	// The queue was empty before we entered this
        	// job in it. Register this with this job,
        	// so that we can give feedback to the master.
                queueEmptyInterval = now - queueEmptyMoment;
                idleTime += queueEmptyInterval;
                queueEmptyMoment = 0L;
            }
            msg.setQueueEmptyInterval( queueEmptyInterval );
            queue.add( msg );
            queue.notifyAll();
        }
    }

    private void handleJobResultMessage( JobResultMessage result )
    {
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Received a job result " + result );
        }
        if( completionListener != null ) {
            completionListener.jobCompleted( result.jobId, result.result );
        }
    }

    /**
     * @param msg The time sync message to handle.
     */
    private void handleTimeSyncMessage( MasterTimeSyncMessage msg )
    {
        ReceivePortIdentifier master = msg.source;
        WorkerTimeSyncMessage m = new WorkerTimeSyncMessage( receivePort.identifier() );
        if( Settings.writeTrace ) {
            Globals.tracer.traceSentMessage( m, receivePort.identifier() );
        }
        try {
            sendPort.send( m, master );
        }
        catch( IOException x ){
            Globals.log.reportError( "Cannot send time sync reply to master " + master );
            x.printStackTrace( Globals.log.getPrintStream() );
        }
    }

    /**
     * Handles job request message <code>msg</code>.
     * @param p The port on which the packet was received.
     * @param msg The job we received and will put in the queue.
     */
    public void messageReceived( PacketUpcallReceivePort<MasterMessage> p, MasterMessage msg )
    {
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Worker: received message " + msg );
        }
        if( Settings.writeTrace ) {
            Globals.tracer.traceReceivedMessage( msg, p.identifier() );
        }
        if( msg instanceof RunJobMessage ){
            RunJobMessage runJobMessage = (RunJobMessage) msg;
    
            handleRunJobMessage( runJobMessage );
        }
        else if( msg instanceof JobResultMessage ) {
            JobResultMessage result = (JobResultMessage) msg;

            handleJobResultMessage( result );
        }
        else if( msg instanceof MasterTimeSyncMessage ){
            MasterTimeSyncMessage ping = (MasterTimeSyncMessage) msg;
    
            handleTimeSyncMessage( ping );
        }
        else if( msg instanceof AddNeighborsMessage ){
            AddNeighborsMessage addMsg = (AddNeighborsMessage) msg;
    
            handleAddNeighborsMessage( addMsg );
        }
        else {
            Globals.log.reportInternalError( "FIXME: handle " + msg );
        }
    }

    /** Runs this worker. */
    @Override
    public void run()
    {
        System.out.println( "Starting worker thread" );
        for( int i=0; i<numberOfProcessors; i++ ) {
            Service.waitToTerminate( workThreads[i] );
            System.out.println( "Ended work thread " + i );
        }
        stopTime = System.nanoTime();
        System.out.println( "End of worker thread" );
    }

    /**
     * Stop this worker.
     */
    public synchronized void setStopped()
    {
        stopped = true;
        synchronized( queue ) {
            queue.notifyAll();
        }
        System.out.println( "Worker is set to stopped" );
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis( IbisIdentifier theIbis )
    {
        synchronized( jobSources ) {
            jobSources.remove( theIbis );
        }
        // This is a good reason to wake up the queue.
        synchronized (queue ) {
            queue.notifyAll();
        }
    }

    /** Gets a job to execute.
     * @return The next job to execute.
     */
    @Override
    public RunJobMessage getJob()
    {
        while( true ) {
            boolean askForWork = false;
            try {
                synchronized( queue ) {
                    if( queue.isEmpty() ) {
                	if( queueEmptyMoment == 0 ) {
                	    queueEmptyMoment = System.nanoTime();
                	}
                        if( isStopped() && runningJobs == 0 ) {
                            // No jobs in queue, and worker is stopped. Return null to
                            // indicate that there won't be further jobs.
                            break;
                        }
                        boolean areJobSources;
                        synchronized( jobSources ){
                            areJobSources = !jobSources.isEmpty();
                        }
                        if( areJobSources ){
                            askForWork = true;
                        }
                        else {
                            // There was no new master to subscribe to.
                            if( Settings.traceWorkerProgress ) {
                                System.out.println( "Worker: waiting for new jobs in queue" );
                            }
                            queue.wait();
                        }
                    }
                    else {
                        runningJobs++;
                        RunJobMessage job = queue.remove();
                        long now = System.nanoTime();
                        job.setRunTime( now );
                        if( Settings.traceWorkerProgress ) {
                            System.out.println( "Worker: handed out job " + job + "; it was queued for " + Service.formatNanoseconds( now-job.getQueueTime() ) + "; there are now " + runningJobs + " running jobs" );
                        }
                        return job;
                    }
                }
                if( askForWork ){
                    findNewMaster();
                }
            }
            catch( InterruptedException e ){
                // Not interesting.
            }
        }
        return null;
    }

    /** Reports the result of the execution of a job. (Overrides method in superclass.)
     * @param jobMessage The job that was run run.
     */
    @Override
    public void reportJobCompletion( RunJobMessage jobMessage )
    {
        long now = System.nanoTime();
        try {
            long queueInterval = jobMessage.getRunTime()-jobMessage.getQueueTime();

            synchronized( queue ){
                queueTime += queueInterval;
                workTime += now-jobMessage.getRunTime();
                jobCount++;
                runningJobs--;
                queue.notifyAll();
            }
            WorkerMessage msg = new WorkerStatusMessage( receivePort.identifier(), jobMessage.jobId );
            if( Settings.writeTrace ) {
                Globals.tracer.traceSentMessage( msg, receivePort.identifier() );
            }
            final ReceivePortIdentifier master = jobMessage.getResultPort();
            sendPort.send( msg, master );
            addJobSource( master.ibisIdentifier() );
            if( Settings.traceWorkerProgress ) {
        	System.out.println( "Completed job "  + jobMessage );
            }
        }
        catch( IOException x ){
            // Something went wrong in sending the result back.
            Globals.log.reportError( "Worker failed to send job result" );
            x.printStackTrace( Globals.log.getPrintStream() );
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

    /** Print some statistics about the entire worker run. */
    public void printStatistics()
    {
	if( stopTime<startTime ) {
	    System.err.println( "Worker didn't stop yet" );
	}
	long workInterval = stopTime-startTime;
	double idlePercentage = 100.0*((double) idleTime/(double) workInterval);
	double workPercentage = 100.0*((double) workTime/(double) workInterval);
	System.out.printf( "Worker: # threads        = %5d\n", workThreads.length );
        System.out.printf( "Worker: # jobs           = %5d\n", jobCount );
        System.out.println( "Worker: run time         = " + Service.formatNanoseconds( workInterval ) );
        System.out.println( "Worker: total work time  = " + Service.formatNanoseconds( workTime ) + String.format( " (%.1f%%)", workPercentage )  );
        System.out.println( "Worker: total idle time  = " + Service.formatNanoseconds( idleTime ) + String.format( " (%.1f%%)", idlePercentage ) );
        if( jobCount>0 ) {
            System.out.println( "Worker: queue time/job   = " + Service.formatNanoseconds( queueTime/jobCount ) );
            System.out.println( "Worker: compute time/job = " + Service.formatNanoseconds( workTime/jobCount ) );
        }
    }
}
