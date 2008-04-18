package ibis.maestro;

import java.io.Serializable;

/**
 * The representation of a job instance.
 * @author Kees van Reeuwijk
 *
 */
class JobInstance implements Serializable {
    private static final long serialVersionUID = -5669565112253289488L;
    final TaskInstanceIdentifier taskInstance;
    final JobType type;
    final Object input;

    /**
     * @param tii The task this job belongs to.
     * @param type The type of this job instance.
     * @param input The input for this job.
     */
    JobInstance(TaskInstanceIdentifier tii, JobType type, Object input) {
	taskInstance = tii;
	this.type = type;
	this.input = input;
    }
    
    @Override
    public String toString()
    {
        return "(job instance: task instance=" + taskInstance + " type=" + type + " input=" + input + ")";
    }
}
