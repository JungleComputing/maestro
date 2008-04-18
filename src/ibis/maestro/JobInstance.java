package ibis.maestro;

import java.io.Serializable;

/**
 * The representation of a job instance.
 * @author Kees van Reeuwijk
 *
 */
public class JobInstance implements Serializable {
    private static final long serialVersionUID = -5669565112253289488L;
    final TaskInstanceIdentifier taskInstance;
    final JobType type;
    final Object input;

    /**
     * @param tii 
     * @param type
     * @param input The input for this job.
     */
    JobInstance(TaskInstanceIdentifier tii, JobType type, Object input) {
	taskInstance = tii;
	this.type = type;
	this.input = input;
    }
}
