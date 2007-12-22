package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * Tell the worker to execute the message contained in this message.
 * 
 * @author Kees van Reeuwijk
 *
 * @param <T> The type of a job result.
 */
public class RunJobMessage<T> extends JobMessage {
    /** */
    private static final long serialVersionUID = 1L;
    private final Job<T> job;
    private final long id;
    private final ReceivePortIdentifier resultPort;

    RunJobMessage( Job<T> job, long id, ReceivePortIdentifier resultPort )
    {
	this.job = job;
	this.id = id;
	this.resultPort = resultPort;
    }

    /** Returns the job contained in this job message.
     * @return The job.
     */
    public Job<T> getJob() {
        return job;
    }
    
    /** Returns the id of this job. */
    public long getId() {
	return id;
    }

    public ReceivePortIdentifier getResultPort() {
        return resultPort;
    }

}
