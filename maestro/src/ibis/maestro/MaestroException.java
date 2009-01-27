package ibis.maestro;

/**
 * The base class of Maestro exceptions.
 * 
 * @author Kees van Reeuwijk.
 */
public class MaestroException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new MaestroException.
     */
    public MaestroException() {
        super();
    }

    /**
     * Given an error message, constructs a new MaestroException.
     * 
     * @param msg
     *            The error message associated with the exception.
     */
    public MaestroException(String msg) {
        super(msg);
    }

    /**
     * Given a throwable, constructs a new MaestroException.
     * 
     * @param t
     *            The throwable that caused the throwing of this exception.
     */
    public MaestroException(Throwable t) {
        super(t);
    }

    /**
     * Given an error message and a throwable, constructs a new
     * MaestroException.
     * 
     * @param msg
     *            The error message associated with the exception.
     * @param t
     *            The throwable associated with the exception.
     */
    public MaestroException(String msg, Throwable t) {
        super(msg, t);
    }

}