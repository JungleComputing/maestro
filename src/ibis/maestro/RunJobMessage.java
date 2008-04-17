package ibis.maestro;

import ibis.maestro.Master.WorkerIdentifier;


/**
 * Tell the worker to execute the job contained in this message.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RunJobMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = 1L;
    final WorkerIdentifier workerIdentifier;
    final Job job;
    final long jobId;
    private transient long queueTime = 0;
    private transient long runTime = 0;
    final TaskInstanceIdentifier taskIdentifier;

    /**
     * Given a job and its source, constructs a new RunJobMessage.
     * @param source Who sent this job, as an identifier we know about.
     * @param job The job to run.
     * @param jobId The identifier of the job.
     */
    RunJobMessage( Worker.MasterIdentifier source, WorkerIdentifier workerIdentifier, Job job, long jobId, TaskInstanceIdentifier taskId )
    {
	super( source );
	this.workerIdentifier = workerIdentifier;
	this.job = job;
        this.jobId = jobId;
        this.taskIdentifier = taskId;
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
}
