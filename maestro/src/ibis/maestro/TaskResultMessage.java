package ibis.maestro;

/**
 * A message containing the result of an entire task.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class TaskResultMessage extends WorkerMessage {
    private static final long serialVersionUID = 5158569253342276404L;
    final TaskInstanceIdentifier task;
    final Object result;

    /**
     * Constructs a new result message.
     * @param task The identifier of the task this is a result for.
     * @param result The result value.
     */
    public TaskResultMessage( TaskInstanceIdentifier task, Object result )
    {
	super( null );
	this.task = task;
	this.result = result;
    }

}
