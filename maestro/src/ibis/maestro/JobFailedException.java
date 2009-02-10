package ibis.maestro;

/**
 * Exception thrown by a task if it can no longer execute this task.
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
