package ibis.maestro;

/**
 * A message containing the result of an entire task.
 * 
 * @author Kees van Reeuwijk
 *
 */
public final class ResultMessage extends WorkerMessage {
    private static final long serialVersionUID = 5158569253342276404L;
    final TaskIdentifier id;
    final JobResultValue result;

    /**
     * Constructs a new result message.
     * @param id The identifier of the task this is a result for.
     * @param result The result value.
     */
    public ResultMessage( TaskIdentifier id, JobResultValue result )
    {
	super( null );
	this.id = id;
	this.result = result;
    }

}
