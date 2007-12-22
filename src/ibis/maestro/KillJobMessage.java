package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message that tells the worker to kill the given job.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class KillJobMessage extends JobMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final long id;
    private final ReceivePortIdentifier resultPort;
    /**
     * @param id
     * @param resultPort
     */
    public KillJobMessage(long id, ReceivePortIdentifier resultPort) {
	this.id = id;
	this.resultPort = resultPort;
    }
    
    /** Returns the id of the job to be killed.
     * Together with the result port of the job, this uniquely identifies
     * the job.
     * @return the Job id.
     */
    public long getId() {
        return id;
    }
    
    /** Returns the result port for the job to be killed.
     * Together with the id of the job, this uniquely identifies
     * the job.
     * @return The result port.
     */
    public ReceivePortIdentifier getResultPort() {
        return resultPort;
    }
}
