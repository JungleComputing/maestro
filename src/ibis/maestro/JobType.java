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

    /** Constructs a new job type.
     * 
     * @param name The name of the type.
     */
    public JobType(String name) {
	this.name = name;
    }
    
    /**
     * Returns a string representation of this type.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "job type " + name;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    /**
     * Returns true iff the given jobtype is equal to this one.
     */
    @Override
    public boolean equals( Object other )
    {
        if( other instanceof JobType ){
            return this.name.equals( ((JobType) other).name );
        }
        return false;
    }
}
