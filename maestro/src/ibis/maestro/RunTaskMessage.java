package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.ArrayList;

/**
 * Tell the worker to execute the task contained in this message.
 * 
 * @author Kees van Reeuwijk
 * 
 */
final class RunTaskMessage extends Message {
    /** */
    private static final long serialVersionUID = 1L;

    final IbisIdentifier workerIdentifier;

    final TaskInstance taskInstance;

    final long taskId;

    final IbisIdentifier source;

    final ArrayList<AntPoint> antTrail;

    /**
     * Given a task and its source, constructs a new RunTaskMessage.
     * 
     * @param source
     *            Who sent this task, as an identifier we know about.
     * @param task
     *            The task to run.
     * @param taskId
     *            The identifier of the task.
     */
    RunTaskMessage(IbisIdentifier workerIdentifier, TaskInstance task,
            long taskId, ArrayList<AntPoint> antTrail) {
        this.source = Globals.localIbis.identifier();
        this.workerIdentifier = workerIdentifier;
        this.taskInstance = task;
        this.taskId = taskId;
        this.antTrail = antTrail;
    }

    /**
     * Returns a string representation of this task message.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "Task message for task " + taskId + " of type "
                + taskInstance.type;
    }

    String label() {
        return taskInstance.shortLabel();
    }
}
