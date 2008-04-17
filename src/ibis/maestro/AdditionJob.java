package ibis.maestro;

/** A Maestro job that multiplies the array of values it is given. */
public class AdditionJob implements Job {
    private int level = 0;
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    AdditionJob( int n )
    {
    }
    
    private static class AdditionData {
        private static double sum = 0.0;
        private final double values[];        
    }

    /**
     * Runs this job.
     */
    @Override
    public void run( AdditionData data, Node node, TaskInstanceIdentifier taskid )
    {
    }

    /**
     * Returns a string representation of this multiply job.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "(AdditionJob level=" + level + " [" + values[0] + ",...," + values[values.length-1] + "])";
    }

    static JobType buildJobType( int level )
    {
	return new JobType( level, "AdditionJob" );
    }

    /**
     * Returns the type of this job.
     * @return The type of this job.
     */
    @Override
    public JobType getType() {
	return buildJobType(level);
    }
}
