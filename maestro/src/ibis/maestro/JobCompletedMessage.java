package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * A message from the worker to the master, telling the master that the worker
 * has completed the given task.
 * 
 * @author Kees van Reeuwijk
 */
final class JobCompletedMessage extends Message {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** The identifier of the task. */
    final long taskId;

    /**
     * The time in seconds this task spent on the worker, from arrival of work
     * to transmission of this message.
     */
    final double workerDwellTime;

    final IbisIdentifier source;

    /**
     * Constructs a task-completed message for the master of a task.
     * 
     * @param taskId
     *            The identifier of the task, as handed out by the master.
     * @param workerDwellTime
     * The time in seconds this task spent on the worker.
     */
    JobCompletedMessage(long taskId, double workerDwellTime) {
        source = Globals.localIbis.identifier();
        this.taskId = taskId;
        this.workerDwellTime = workerDwellTime;
    }

    /**
     * Returns a string representation of this status message.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "task completed message: taskId=" + taskId + " workerDwellTime="
                + workerDwellTime;
    }
}
