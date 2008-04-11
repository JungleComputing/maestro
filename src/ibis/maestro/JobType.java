package ibis.maestro;

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
    final String name;
    final int priority;

    /** Constructs a new job type.
     * 
     * @param priority The priority of this job.
     * @param name The name of the type.
     */
    public JobType( int priority, String name) {
	this.priority = priority;
	this.name = name;
    }
    
    /**
     * Returns a string representation of this type.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "job type " + name + "(priority=" + priority + ")";
    }

    /**
     * Compute a hash code for this job type.
     * Since we compare on name, we can simply use the hash of the name.
     * @return The hash code.
     */
    @Override
    public int hashCode()
    {
        return priority ^ name.hashCode();
    }

    /**
     * Returns true iff the given jobtype is equal to this one.
     */
    @Override
    public boolean equals( Object o )
    {
        if( o instanceof JobType ){
            JobType other = (JobType) o;
            if( other.priority != this.priority ){
        	return false;
            }
	    return this.name.equals( other.name );
        }
        return false;
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
	if( a.priority>b.priority ) {
	    return 1;
	}
	if( a.priority<b.priority ) {
	    return -1;
	}
	return 0;
    }
}
