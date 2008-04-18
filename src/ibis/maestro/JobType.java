package ibis.maestro;

import ibis.maestro.Task.TaskIdentifier;

import java.io.Serializable;

/**
 * The interface of a class that represents a job type.
 * @author Kees van Reeuwijk
 *
 */
public class JobType implements Serializable
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
    public JobType( TaskIdentifier id, int jobNo)
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
     * Compute a hash code for this job type.
     * Since we compare on name, we can simply use the hash of the name.
     * @return The hash code.
     */
    @Override
    public int hashCode()
    {
        return task.hashCode() ^ jobNo;
    }

	/**
	 * Compares two job types based on priority. Returns
	 * 1 if type a has more priority as b, etc.
	 * @param a One of the job types to compare.
	 * @param b The other job type to compare.
	 * @return The comparison result.
	 */
    public static int comparePriorities( JobType a, JobType b )
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
