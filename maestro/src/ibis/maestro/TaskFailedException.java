package ibis.maestro;

/**
 * Exception thrown by a task if it can no longer execute this task.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskFailedException extends MaestroException
{
    private static final long serialVersionUID = 1L;

    /**
     * Given FIXME, constructs a new MaestroException.
     */
    public TaskFailedException()
    {
        super();
    }

    /**
     * Given FIXME, constructs a new MaestroException.
     * @param arg0
     */
    public TaskFailedException( String arg0 )
    {
        super( arg0 );
    }

    /**
     * Given FIXME, constructs a new MaestroException.
     * @param arg0
     */
    public TaskFailedException( Throwable arg0 )
    {
        super( arg0 );
    }

    /**
     * Given FIXME, constructs a new MaestroException.
     * @param arg0
     * @param arg1
     */
    public TaskFailedException( String arg0, Throwable arg1 )
    {
        super( arg0, arg1 );
    }

}
