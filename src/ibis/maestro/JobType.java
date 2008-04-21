package ibis.maestro;

import ibis.maestro.Task.TaskIdentifier;

import java.io.Serializable;

/**
 * A job type.
 * 
 * @author Kees van Reeuwijk
 */
class JobType implements Serializable
{
    /** Contractual obligation. */
    private static final long serialVersionUID = 13451L;
    final int jobNo;
    final TaskIdentifier task;

    /** Constructs a new job type.
     * 
     * @param id The task this job belongs to.
     * @param jobNo The sequence number within the task.
     */
    JobType( TaskIdentifier id, int jobNo)
    {
        this.task = id;
        this.jobNo = jobNo;
    }

    /**
     * Returns a string representation of this type.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "(task=" + task + ",jobNo=" + jobNo + ")";
    }

    /**
     * Returns the hash code of this job type.
     * @return The hash code.
     */
    @Override
    public int hashCode() {
        return task.hashCode()*100 + jobNo;
    }

    /**
     * Returns true iff the given object is a job type that is equal
     * to this one.
     * @param obj The object to compare to.
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
        if (task == null) {
            if (other.task != null)
                return false;
        } else if (!task.equals(other.task))
            return false;
        return true;
    }

    /**
     * Compares two job types based on priority. Returns
     * 1 if type a has more priority as b, etc.
     * @param a One of the job types to compare.
     * @param b The other job type to compare.
     * @return The comparison result.
     */
    static int comparePriorities( JobType a, JobType b )
    {
        if( a.jobNo>b.jobNo ) {
            return 1;
        }
        if( a.jobNo<b.jobNo ) {
            return -1;
        }
        return 0;
    }
}
