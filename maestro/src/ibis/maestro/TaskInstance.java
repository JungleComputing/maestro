package ibis.maestro;

import java.io.Serializable;

/**
 * The representation of a task instance.
 * @author Kees van Reeuwijk
 *
 */
class TaskInstance implements Serializable {
    private static final long serialVersionUID = -5669565112253289488L;
    final JobInstanceIdentifier jobInstance;
    final TaskType type;
    final Object input;

    /**
     * @param tii The job this task belongs to.
     * @param type The type of this task instance.
     * @param input The input for this task.
     */
    TaskInstance( JobInstanceIdentifier tii, TaskType type, Object input) {
	jobInstance = tii;
	this.type = type;
	this.input = input;
    }

    String formatJobAndType()
    {
	return "(jobId=" + jobInstance.id + ",type=" + type + ")";
    }
    /**
     * Returns a string representation of this task instance.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "(task instance: job instance=" + jobInstance + " type=" + type + " input=" + input + ")";
    }
}
