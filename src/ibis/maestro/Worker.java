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
public final class Worker extends Thread implements WorkSource, PacketReceiveListener<MasterMessage> {
    private final Node node;
    
    /** The list of all known masters, in the order that they were handed their
     * id. Thus, element <code>i</code> of the list is guaranteed to have
     * <code>i</code> as its id; otherwise the entry is empty.
     */
    private final ArrayList<MasterInfo> masters = new ArrayList<MasterInfo>();
    
    /** The list of masters we should ask for (extra) work if we are bored. */
    private final ArrayList<MasterInfo> jobSources = new ArrayList<MasterInfo>();

    /** The list of job types we know how to handle. */
    private final ArrayList<JobType> allowedTypes;

    private final PacketUpcallReceivePort<MasterMessage> receivePort;
    private final PacketSendPort<WorkerMessage> sendPort;
    private final CompletionListener completionListener;
    private int exclusiveMaster = -1;
    private long queueEmptyMoment = 0L;
    private final LinkedList<RunJobMessage> queue = new LinkedList<RunJobMessage>();
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
    private int jobSettleCount = 0;
    private final Random rng = new Random();

    /**
     * Create a new Maestro worker instance using the given Ibis instance.
     * @param ibis The Ibis instance this worker belongs to.
     * @param node The node this worker belongs to.
     * @param master The master that jobs may submit new jobs to.
     * @param allowedTypes The types of job this worker can handle.
     * @param completionListener The listener for job completion reports.
     * @throws IOException Thrown if the construction of the worker failed.
     */
    public Worker( Ibis ibis, Node node, Master master, ArrayList<JobType> allowedTypes, CompletionListener completionListener ) throws IOException
    {
        super( "Worker" );   // Create a thread with a name.
        this.node = node;
        this.allowedTypes = allowedTypes;
        this.completionListener = completionListener;
        receivePort = new PacketUpcallReceivePort<MasterMessage>( ibis, Globals.workerReceivePortName, this );
        sendPort = new PacketSendPort<WorkerMessage>( ibis );
        for( int i=0; i<numberOfProcessors; i++ ) {
            WorkThread t = new WorkThread( this, master );
            workThreads[i] = t;
            t.start();
        }
        startTime = System.nanoTime();
        receivePort.enable();   // We're open for business.
    }


    /** Given a master identifier, returns the master info
     * for the master with that id, or null if there is no such
     * master (any more).
     * @param masterID The master id to search for.
     * @return The master info, or null if there is no such master.
     */
    private MasterInfo getMasterInfo( int master )
    {
	synchronized( masters ) {
	    return masters.get( master );
	}
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

    private synchronized boolean isStopped()
    {
        return stopped;
    }

    /**
     * Returns the identifier of the job submission port of this worker.
     * @return The port identifier.
     */
    ReceivePortIdentifier identifier()
    {
        return receivePort.identifier();
    }

    private synchronized void setExclusiveMaster( int port )
    {
	exclusiveMaster = port;
    }

    /** Removes and returns a random job source from the list of
     * known job sources. Returns null if the list is empty.
     * 
     * @return The job source, or null if there isn't one.
     */
    private MasterInfo getRandomJobSource()
    {
        MasterInfo res;

        synchronized( this ) {
            if( exclusiveMaster>=0 ) {
        	return null;
            }
        }
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

    /**
     * A new ibis has joined the computation.
     * @param theIbis The ibis that has joined.
     */
    void addJobSource( IbisIdentifier theIbis )
    {
        MasterInfo mi;
        synchronized( masters ) {
            mi = new MasterInfo( masters.size(), -1, theIbis );
            masters.add( mi );
        }
        synchronized( jobSources ){
            jobSources.add( mi );
        }
        // This is a good reason to wake up the queue.
        synchronized (queue ) {
            queue.notifyAll();
        }
    }

    private void findNewMaster()
    {
	synchronized( queue ) {
	    if( jobSettleCount>0 ) {
		return;
	    }
	}
        MasterInfo m = getRandomJobSource();
        if( m == null ){
            return;
        }
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Asking neighbor " + m.identifier + " for work" );
        }
        try {
            WorkRequestMessage msg = new WorkRequestMessage( receivePort.identifier(), m.identifier, allowedTypes );
    
            sendPort.send( msg, m.ibis, Globals.masterReceivePortName, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
            synchronized( queue ) {
        	jobSettleCount = 4; // FIXME: symbolize this magic number.
            }
        }
        catch( IOException x ){
            Globals.log.reportError( "Failed to send a work request message to neighbor " + m );
            x.printStackTrace();
        }
    }

    /** Tell the given master that we won't do its work any more.
     * 
     * @param source The master to resign from.
     */
    private void sendResignMessage( int master )
    {
	MasterInfo mi = getMasterInfo( master );
	if( mi != null ) {
	    WorkerResignMessage msg = new WorkerResignMessage( mi.identifierWithMaster );
	    try {
		sendPort.send( msg, master, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
	    } catch (IOException e) {
		// We can't send a resign message. Oh well.
	    }
        }
    }

    /**
     * Handle a message containing a new job to run.
     * 
     * @param msg The message to handle.
     */
    private void handleRunJobMessage( RunJobMessage msg )
    {
	boolean sendResignation = false;
        long now = System.nanoTime();

        msg.setQueueTime( now );
        synchronized( this ) {
            if( exclusiveMaster>=0 && msg.source != exclusiveMaster ) {
        	sendResignation = true;
            }
        }
        if( sendResignation ) {
            // We're closing, please go away.
            sendResignMessage( msg.source );
        }
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
            synchronized( completionListener ) {
        	completionListener.jobCompleted( node, result.jobId, result.result );
            }
        }
    }

    private void handleWorkerAcceptMessage( WorkerAcceptMessage msg )
    {
        if( Settings.traceWorkerProgress ){
            Globals.log.reportProgress( "Received a worker accept message " + msg );
        }
        sendPort.registerDestination( msg.port, msg.source );
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
        if( msg instanceof RunJobMessage ){
            RunJobMessage runJobMessage = (RunJobMessage) msg;
    
            handleRunJobMessage( runJobMessage );
        }
        else if( msg instanceof JobResultMessage ) {
            JobResultMessage result = (JobResultMessage) msg;

            handleJobResultMessage( result );
        }
        else if( msg instanceof WorkerAcceptMessage ) {
            WorkerAcceptMessage am = (WorkerAcceptMessage) msg;

            handleWorkerAcceptMessage( am );
        }
        else {
            Globals.log.reportInternalError( "FIXME: handle messages of type " + msg.getClass() );
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
        	boolean stopping = isStopped();

        	synchronized( queue ) {
                    if( queue.isEmpty() ) {
                	if( queueEmptyMoment == 0 ) {
                	    queueEmptyMoment = System.nanoTime();
                	}
                	if( stopping && runningJobs == 0 ) {
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
                        if( jobSettleCount>0 ) {
                            jobSettleCount--;
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
    
            WorkerMessage msg = new WorkerStatusMessage( jobMessage.workerIdentifier, jobMessage.jobId );
            final int master = jobMessage.source;
            sendPort.send( msg, master, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
            final MasterInfo mi = getMasterInfo( master );
            if( mi != null ) {
        	synchronized( jobSources ) {
        	    jobSources.add( mi );
        	}
            }
            if( Settings.traceWorkerProgress ) {
        	System.out.println( "Completed job "  + jobMessage );
            }
            synchronized( queue ){
                queueTime += queueInterval;
                workTime += now-jobMessage.getRunTime();
                jobCount++;
                runningJobs--;
                queue.notifyAll();
            }
        }
        catch( IOException x ){
            // Something went wrong in sending the result back.
            Globals.log.reportError( "Worker failed to send job result" );
            x.printStackTrace( Globals.log.getPrintStream() );
        }
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis( IbisIdentifier theIbis )
    {
        synchronized( jobSources ) {
            int ix = jobSources.size();
            while( ix>0 ){
                ix--;

                MasterInfo ji = jobSources.get( ix );
                if( ji.ibis.equals( theIbis ) ){
                    jobSources.remove( ix );
                }
            }
        }
        // This is a good reason to wake up the queue.
        synchronized (queue ) {
            queue.notifyAll();
        }
    }

    /** Runs this worker. */
    @Override
    public void run()
    {
        for( int i=0; i<numberOfProcessors; i++ ) {
            Service.waitToTerminate( workThreads[i] );
        }
        stopTime = System.nanoTime();
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
        sendPort.printStats( "worker send port" );
        if( jobCount>0 ) {
            System.out.println( "Worker: queue time/job   = " + Service.formatNanoseconds( queueTime/jobCount ) );
            System.out.println( "Worker: compute time/job = " + Service.formatNanoseconds( workTime/jobCount ) );
        }
    }
}
