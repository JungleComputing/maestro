package ibis.maestro;

import java.io.Serializable;

/**
 * The representation of a job instance.
 * @author Kees van Reeuwijk
 *
 */
public class JobInstance implements Serializable {
    final JobType type;
    final Object input;

    /**
     * @param type
     * @param input
     */
    public JobInstance(JobType type, Object input) {
	this.type = type;
	this.input = input;
    }
}
