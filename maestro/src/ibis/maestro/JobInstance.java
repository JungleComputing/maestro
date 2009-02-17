package ibis.maestro;

import java.io.Serializable;

/**
 * The representation of a job instance.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class JobInstance implements Serializable {
    private static final long serialVersionUID = -5669565112253289488L;

    final JobInstanceIdentifier jobInstance;

    final JobType type;

    final Object input;

    private boolean orphan = false;

    /**
     * @param tii
     *            The job sequence this job belongs to.
     * @param type
     *            The type of this job instance.
     * @param input
     *            The input for this job.
     */
    JobInstance(JobInstanceIdentifier tii, JobType type, Object input) {
        jobInstance = tii;
        this.type = type;
        this.input = input;
    }

    String formatJobAndType() {
        return "(jobId=" + jobInstance.id + ",type=" + type + ")";
    }

    /**
     * Returns a string representation of this job instance.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(job instance: job instance=" + jobInstance + " type=" + type
                + " input=" + input + ")";
    }

    String shortLabel() {
        return jobInstance.label() + "#" + type;
    }

    void setOrphan() {
        orphan = true;
    }

    boolean isOrphan() {
        return orphan;
    }

    /**
     * Returns the hash code of this job instance.
     * 
     * @return The hash code.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime 
                + ((jobInstance == null) ? 0 : jobInstance.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    /**
     * Determines whether the given object equals this job instance.
     * 
     * @param obj
     *            The object to compare with.
     * @return True iff the given object equals this job instance.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final JobInstance other = (JobInstance) obj;
        if (jobInstance == null) {
            if (other.jobInstance != null)
                return false;
        } else if (!jobInstance.equals(other.jobInstance))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }

}
