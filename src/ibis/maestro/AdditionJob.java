package ibis.maestro;

/** A Maestro job that multiplies the array of values it is given. */
public class AdditionJob implements Job {
    private static final int BLOCK_SIZE = 4000;
    private static final int ITERATIONS = 5000;  // The number of times we should do the addition.
    static final int LEVELS = 4;
    private int level = 0;
    private static double sum = 0.0;
    private final double values[];
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    AdditionJob( int n )
    {
	double a[] = new double [BLOCK_SIZE];
	for( int i=0; i<BLOCK_SIZE; i++ ) {
	    a[i] = i+n;
	}
	this.values = a;
    }

    /**
     * Runs this job.
     */
    @Override
    public void run( Node node, TaskIdentifier taskid )
    {
	for( int i=0; i<ITERATIONS; i++ ) {
	    for( double v: values ) {
		sum += v;
	    }
	}
	if( level<LEVELS ) {
	    level++;
	    node.submit( this, taskid );
	}
	else {
	    JobResultValue result = new DoubleResultValue( sum );
	    taskid.reportResult( node, result );
	}
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
