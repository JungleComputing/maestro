package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A message telling the master that a worker failed to execute the given task.
 * It also contains information to let the master update its administration,
 * so that it won't send the task to the same worker again.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class TaskFailMessage extends Message
{
    private static final long serialVersionUID = 5158569253342276404L;
    final IbisIdentifier source;
    final long id;

    /**
     * Constructs a new result message.
     * @param id The identifier of the job this is a result for.
     * @param update Information about the new state of the node.
     */
    public TaskFailMessage( long id )
    {
        this.source = Globals.localIbis.identifier();
	this.id = id;
    }

}
