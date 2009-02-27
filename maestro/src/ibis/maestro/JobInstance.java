package ibis.maestro;

import java.io.Serializable;
import java.util.Arrays;

/**
 * The representation of a job instance.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class JobInstance implements Serializable {
    private static final long serialVersionUID = -5669565112253289488L;

    final JobInstanceIdentifier jobInstance;

    final Object input;

    final JobType todoList[];

    private boolean orphan = false;

    /**
     * @param tii
     *            The identifier of this job instance.
     * @param type
     *            The type of this job instance.
     * @param input
     *            The input for this job.
     *            @param todoList
     *            The list of jobs to do on the input.
     */
    JobInstance(JobInstanceIdentifier tii, Object input,JobType todoList[]) {
        this.jobInstance = tii;
        this.input = input;
        this.todoList = todoList;
    }

    String formatJobAndType() {
        return "(jobId=" + jobInstance.id + ",todo=" + Arrays.deepToString(todoList) + ")";
    }

    /**
     * Returns a string representation of this job instance.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(job instance: job instance=" + jobInstance + " todo=" + Arrays.deepToString(todoList)
                + " input=" + input + ")";
    }

    String shortLabel() {
        return jobInstance.label() + "#" + Arrays.deepToString(todoList);
    }

    void setOrphan() {
        orphan = true;
    }

    boolean isOrphan() {
        return orphan;
    }

    JobType getFirstType() {
        return todoList[0];
    }

}
