package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * Tell the worker to execute the message contained in this message.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RunJobMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = 1L;
    private final Job job;
    private final long id;
    private final ReceivePortIdentifier resultPort;

    RunJobMessage( Job job, long id, ReceivePortIdentifier resultPort )
    {
	this.job = job;
	this.id = id;
	this.resultPort = resultPort;
    }

    /** Returns the job contained in this job message.
     * @return The job.
     */
    public Job getJob() {
        return job;
    }
    
    /** Returns the id of this job.
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
        return resultPort;
    }

}
