package ibis.maestro;

/**
 * A message telling the master that a worker failed to execute the given job.
 * It also contains information to let the master update its administration, so
 * that it won't send the job to the same worker again.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class JobFailedMessage extends Message {
    private static final long serialVersionUID = 5158569253342276404L;

    final long id;

    /**
     * Constructs a new result message.
     * 
     * @param id
     *            The identifier of the job this is a result for.
     */
    protected JobFailedMessage(final long id) {
        this.id = id;
    }

}
