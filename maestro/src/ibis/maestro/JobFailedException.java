package ibis.maestro;

/**
 * Exception thrown by a job if it can no longer execute this job.
 * 
 * @author Kees van Reeuwijk.
 */
public class JobFailedException extends MaestroException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new MaestroException.
     */
    public JobFailedException() {
        super();
    }

    /**
     * Given an error message, constructs a new MaestroException.
     * 
     * @param msg
     *            The error message.
     */
    public JobFailedException(String msg) {
        super(msg);
    }

}
