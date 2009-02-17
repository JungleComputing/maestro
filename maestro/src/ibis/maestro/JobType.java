package ibis.maestro;

import ibis.maestro.JobSequence.JobSequenceIdentifier;

import java.io.Serializable;

/**
 * A job type.
 * 
 * @author Kees van Reeuwijk
 */
final class JobType implements Serializable {
    /** Contractual obligation. */
    private static final long serialVersionUID = 13451L;

    final int jobNo;

    final int remainingJobs;

    final int index;

    final JobSequenceIdentifier job;

    final boolean unpredictable;

    /**
     * Constructs a new job type.
     * 
     * @param id
     *            The job sequence this job belongs to.
     * @param jobNo
     *            The sequence number within the job sequence.
     * @param remainingJobs
     *            The number of jobs after this one in the job sequence.
     */
    JobType(JobSequenceIdentifier id, int jobNo, int remainingJobs,
            boolean unpredictable, int index) {
        this.job = id;
        this.jobNo = jobNo;
        this.remainingJobs = remainingJobs;
        this.unpredictable = unpredictable;
        this.index = index;
    }

    /**
     * Returns a string representation of this type.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(" + job.toString() + ",J" + jobNo + ")";
    }

    /**
     * Returns the hash code of this job type.
     * 
     * @return The hash code.
     */
    @Override
    public int hashCode() {
        return job.hashCode() * 100 + jobNo;
    }

    /**
     * Returns true iff the given object is a job type that is equal to this
     * one.
     * 
     * @param obj
     *            The object to compare to.
     * @return True iff this and the given object are equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final JobType other = (JobType) obj;
        if (jobNo != other.jobNo)
            return false;
        if (job == null) {
            if (other.job != null)
                return false;
        } else if (!job.equals(other.job))
            return false;
        return true;
    }

    /**
     * Compares two job types based on priority. Returns 1 if type a has more
     * priority as b, etc.
     * 
     * @param a
     *            One of the job types to compare.
     * @param b
     *            The other job type to compare.
     * @return The comparison result.
     */
    static int comparePriorities(JobType a, JobType b) {
        if (a.jobNo > b.jobNo) {
            return -1;
        }
        if (a.jobNo < b.jobNo) {
            return 1;
        }
        return 0;
    }
}
