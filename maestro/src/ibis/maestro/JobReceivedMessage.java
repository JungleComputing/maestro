package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A message from the worker to the master, telling the master that the worker
 * has received the given task.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class JobReceivedMessage extends Message {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** The identifier of the job. */
    final long taskId;

    final IbisIdentifier source;

    /**
     * Constructs a task-completed message for the master of a task.
     * 
     * @param jobId
     *            The identifier of the job, as handed out by the master.
     */
    JobReceivedMessage(long jobId) {
        source = Globals.localIbis.identifier();
        this.taskId = jobId;
    }

    /**
     * Returns a string representation of this status message.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "task received message: taskId=" + taskId;
    }
}
