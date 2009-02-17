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
    protected MaestroException(String msg) {
        super(msg);
    }

}