 package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.io.IOException;
import java.io.Serializable;
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

    /** The list of ibises we haven't registered with yet. */
    private final LinkedList<IbisIdentifier> unregisteredMasters = new LinkedList<IbisIdentifier>();

    /** The list of masters we should tell about new job types we can handle. */
    private final LinkedList<MasterInfo> mastersToUpdate = new LinkedList<MasterInfo>();

    /** The list of masters we should ask for extra work if we are bored. */
    private final ArrayList<MasterInfo> jobSources = new ArrayList<MasterInfo>();

    /** The list of job types we know how to handle. */
    private final ArrayList<JobType> allowedTypes;

    private final PacketUpcallReceivePort<MasterMessage> receivePort;
    private final PacketSendPort<WorkerMessage> sendPort;
    private final CompletionListener completionListener;
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
    private boolean askForWork = true;
    private final Random rng = new Random();

    static final class MasterIdentifier implements Serializable {
        private static final long serialVersionUID = 7727840589973468928L;
        final int value;
	
	private MasterIdentifier( int value )
	{
	    this.value = value;
	}

	/**
	 * @return
	 */
	@Override
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + value;
	    return result;
	}

	/**
	 * @param obj The object to compare to.
	 * @return True iff the two identifiers are equal.
	 */
	@Override
	public boolean equals(Object obj) {
	    if (this == obj)
		return true;
	    if (obj == null)
		return false;
	    if (getClass() != obj.getClass())
		return false;
	    final MasterIdentifier other = (MasterIdentifier) obj;
	    if (value != other.value)
		return false;
	    return true;
	}
    }

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


    private static boolean member( ArrayList<MasterInfo> l, MasterInfo e )
    {
        for( MasterInfo mi: l ) {
            if( mi == e ) {
        	return true;
            }
        }
        return false;
    }


    /** Given a master identifier, returns the master info
     * for the master with that id, or null if there is no such
     * master (any more).
     * @param master The master id to search for.
     * @return The master info, or null if there is no such master.
     */
    private MasterInfo getMasterInfo( MasterIdentifier master )
    {
	synchronized( queue ) {
	    return masters.get( master.value );
	}
    }

    /**
     * Stop this worker.
     */
    public void setStopped()
    {
        synchronized( queue ) {
            stopped = true;
            queue.notifyAll();
        }
        System.out.println( "Worker is set to stopped" );
    }

    /**
     * Tells this worker not to ask for work any more.
     */
    public void stopAskingForWork()
    {
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "Worker: don't ask for work" );
	}
        synchronized( queue ){
            askForWork = false;
        }
    }

    /**
     * Returns the identifier of the job submission port of this worker.
     * @return The port identifier.
     */
    ReceivePortIdentifier identifier()
    {
        return receivePort.identifier();
    }

    /** Removes and returns a random job source from the list of
     * known job sources. Returns null if the list is empty.
     * 
     * @return The job source, or null if there isn't one.
     */
    private MasterInfo getRandomJobSource()
    {
        MasterInfo res;

        synchronized( queue ){
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
        synchronized( queue ){
            unregisteredMasters.addLast( theIbis );
            // This is a good reason to wake up the queue.
            queue.notifyAll();
        }
    }

    /**
     * Returns the first element of the list of unregistered masters, or
     * <code>null</code> if there is nothing in the list.
     * @return An unregistered master.
     */
    private IbisIdentifier getUnregisteredMaster()
    {
        synchronized( queue ){
            if( unregisteredMasters.isEmpty() ){
                return null;
            }
            return unregisteredMasters.removeFirst();
        }
    }

    private void registerWithMaster( IbisIdentifier ibis )
    {
        MasterIdentifier masterID;

        synchronized( queue ){
            // Reserve a slot for this master, and get an id.
            masterID = new MasterIdentifier( masters.size() );
            MasterInfo info = new MasterInfo( masterID, null, ibis );
            masters.add( info );
        }
        RegisterWorkerMessage msg = new RegisterWorkerMessage( receivePort.identifier(), masterID );
        sendPort.tryToSend( ibis, Globals.masterReceivePortName, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
    }

    /** Returns a master to update, or null if there is no such master.
     * 
     * @return The master to update.
     */
    private MasterInfo getMasterToUpdate()
    {
        synchronized( queue ){
            if( mastersToUpdate.isEmpty() ){
                return null;
            }
            return mastersToUpdate.removeFirst();
        }
    }

    /** Update the given master with our new list of allowed types. */
    private void updateMaster( MasterInfo master )
    {
        JobType jobTypes[];

        synchronized( queue ){
            jobTypes = new JobType[allowedTypes.size()];
            allowedTypes.toArray( jobTypes );
        }
        RegisterTypeMessage msg = new RegisterTypeMessage( master.getIdentifierOnMaster(), jobTypes );
        sendPort.tryToSend( master.localIdentifier.value, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
    }

    private void sendJobRequest( MasterInfo master )
    {
        WorkRequestMessage msg = new WorkRequestMessage( master.getIdentifierOnMaster() );
        sendPort.tryToSend( master.localIdentifier.value, msg, Settings.OPTIONAL_COMMUNICATION_TIMEOUT );
    }
    
    private void askMoreWork()
    {
        // First try to register with a new ibis.
        IbisIdentifier newIbis = getUnregisteredMaster();
        if( newIbis != null ){
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Worker: registering with master " + newIbis );
            }
            registerWithMaster( newIbis );
            return;
        }
        
        // Next, try to tell a master about new job types.
        MasterInfo masterToUpdate = getMasterToUpdate();
        if( masterToUpdate != null ){
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Worker: telling master " + masterToUpdate.localIdentifier + " about new job types" );
            }
            updateMaster( masterToUpdate );
            return;
        }

        synchronized( queue ){
            if( !askForWork ){
                return;
            }
            jobSettleCount--;
            if( jobSettleCount>0 ) {
                return;
            }
        }

        // Finally, try to tell a master we want more jobs.
        MasterInfo jobSource = getRandomJobSource();
        if( jobSource != null ){
            if( Settings.traceWorkerProgress ){
                Globals.log.reportProgress( "Worker: asking master " + jobSource.localIdentifier + " for work" );
            }
            sendJobRequest( jobSource );
            return;
        }
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
        sendPort.registerDestination( msg.port, msg.source.value );
        synchronized( queue ){
            MasterInfo master = masters.get( msg.source.value );
            master.setIdentifierOnMaster( msg.identifierOnMaster );
            mastersToUpdate.addFirst( master );
            queue.notifyAll();
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
        	synchronized( queue ) {
                    if( queue.isEmpty() ) {
                	if( queueEmptyMoment == 0 ) {
                	    queueEmptyMoment = System.nanoTime();
                	}
                	if( stopped && runningJobs == 0 ) {
                            // No jobs in queue, and worker is stopped. Return null to
                            // indicate that there won't be further jobs.
                            break;
                        }
                        if( jobSources.isEmpty() && unregisteredMasters.isEmpty() && mastersToUpdate.isEmpty() ){
                            // There was no master to subscribe to, update, or ask for work.
                            if( Settings.traceWorkerProgress ) {
                                System.out.println( "Worker: waiting for new jobs in queue" );
                            }
                            queue.wait();
                        }
                        else {
                            askForWork = true;
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
                    askMoreWork();
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
        long queueInterval = jobMessage.getRunTime()-jobMessage.getQueueTime();
   
        WorkerMessage msg = new WorkerStatusMessage( jobMessage.workerIdentifier, jobMessage.jobId );
        final MasterIdentifier master = jobMessage.source;
        sendPort.tryToSend( master.value, msg, Settings.ESSENTIAL_COMMUNICATION_TIMEOUT );
        if( Settings.traceWorkerProgress ) {
            System.out.println( "Completed job "  + jobMessage );
        }
        final MasterInfo mi = getMasterInfo( master );
        synchronized( queue ) {
            if( mi != null ) {
        	if( !member( jobSources, mi ) ) {
        	    jobSources.add( mi );
        	}
            }
            queueTime += queueInterval;
            workTime += now-jobMessage.getRunTime();
            jobCount++;
            runningJobs--;
            queue.notifyAll();
        }
    }

    /**
     * We know the given ibis has disappeared from the computation.
     * Make sure we don't talk to it.
     * @param theIbis The ibis that was gone.
     */
    public void removeIbis( IbisIdentifier theIbis )
    {
        synchronized( queue ) {
            int ix = jobSources.size();
            while( ix>0 ){
                ix--;

                MasterInfo ji = jobSources.get( ix );
                if( ji.ibis.equals( theIbis ) ){
                    jobSources.remove( ix );
                }
            }
            // This is a good reason to wake up the queue.
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
