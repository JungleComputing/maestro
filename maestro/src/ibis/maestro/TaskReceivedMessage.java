package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A message from the worker to the master, telling the master that the worker
 * has received the given task.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class TaskReceivedMessage extends Message {
	/** Contractual obligation. */
	private static final long serialVersionUID = 1L;

	/** The identifier of the task. */
	final long taskId;

	final IbisIdentifier source;

	/**
	 * Constructs a task-completed message for the master of a task.
	 * 
	 * @param taskId
	 *            The identifier of the task, as handed out by the master.
	 */
	TaskReceivedMessage(long taskId) {
		source = Globals.localIbis.identifier();
		this.taskId = taskId;
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
