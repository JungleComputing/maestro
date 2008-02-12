package ibis.maestro;

/**
 * The interface of a class that represents a job type.
 * @author Kees van Reeuwijk
 *
 */
public class JobType {
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
}
