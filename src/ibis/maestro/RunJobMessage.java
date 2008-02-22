package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * Tell the worker to execute the job contained in this message.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RunJobMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = 1L;
    final Job job;
    final long jobId;
    private transient long queueTime;
    private transient long runTime;
    private transient long queueEmptyInterval;

    RunJobMessage( Job job, ReceivePortIdentifier resultPort, long jobId )
    {
	super( resultPort );
	this.job = job;
        this.jobId = jobId;
    }

    /**
     * Returns the result port of the run job.
     * @return The port that should be sent the result of this job.
     */
    public ReceivePortIdentifier getResultPort() {
        return source;
    }

    /** Set the start time of this job to the given time in ns.
     * @param t The start time.
     */
    public void setQueueTime(long t) {
        this.queueTime = t;
    }

    /**
     * Registers the given time as the moment this job started running.
     * @param t The start time.
     */
    public void setRunTime(long t )
    {
        this.runTime = t;
    }

    /** Returns the registered enqueueing time.
     * 
     * @return The registered enqueueing time.
     */
    public long getQueueTime() {
        return queueTime;
    }


    /** Returns the registered start time.
     * 
     * @return The registered start time.
     */
    public long getRunTime() {
        return runTime;
    }

    /**
     * Returns a string representation of this job messabge.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "Job message for job " + jobId;
    }


    /**
     * Returns the event type of this message.
     */
    @Override
    protected TransmissionEvent.Type getMessageType()
    {
	return TransmissionEvent.Type.JOB;
    }

    /**
     * Sets the queue empty interval associated with this job.
     * @param t The time interval the worker queue was empty before this
     *          job entered its queue.
     */
    public void setQueueEmptyInterval(long t) {
	this.queueEmptyInterval = t;
    }

    /**
     * Gets the queue empty interval associated with this job.
     * @return The time interval the worker queue was empty before this
     *          job entered its queue.
     */
    public long getQueueEmptyInterval() {
	return queueEmptyInterval;
    }
}
