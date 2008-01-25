package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * Tell the worker to execute the job contained in this message.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RunJobMessage extends MasterMessage implements Comparable<Object> {
    /** */
    private static final long serialVersionUID = 1L;
    private final Job job;
    private transient long startTime;

    RunJobMessage( Job job, ReceivePortIdentifier resultPort )
    {
	super( resultPort );
	this.job = job;
    }

    /** Returns the job contained in this job message.
     * @return The job.
     */
    public Job getJob() {
        return job;
    }

    /** Returns the id of this job. // FIXME: inline this.
     * @return The job ID.
     */
    public long getId() {
	return id;
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
    public void setStartTime(long t) {
        startTime = t;
    }

    /** Returns the registered start time.
     * 
     * @return The registered start time.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns a string representation of this job messabge.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "Job message for job " + id;
    }

    /**
     * Compares this run job to the given RunJobMessage.
     * @param o The RunJobMessage to compare to.
     * @return The comparison result.
     */
    public int compareTo(Object o) {
        RunJobMessage other = (RunJobMessage) o;
        int res = this.job.compareTo( other.job );
        if( res != 0 ){
            return res;
        }
        if( this.id>other.id ){
            return 1;
        }
        if( this.id<other.id ){
            return -1;
        }
        return 0;
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TraceEvent.Type getMessageType()
    {
	return TraceEvent.Type.JOB;
    }
}
