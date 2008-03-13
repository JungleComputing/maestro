package ibis.maestro;

/** A Maestro job that multiplies the array of values it is given. */
public class AdditionJob implements Job {
    private static final int BLOCK_SIZE = 4000;
    private static final int ITERATIONS = 3000;  // The number of times we should do the addition.
    private static final int LEVELS = 4;
    private int level = 0;
    private static double sum = 0.0;
    private final double values[];
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    static final JobType jobType = new JobType( "AdditionJob" );

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
    public void run( JobContext master )
    {
	for( int i=0; i<ITERATIONS; i++ ) {
	    for( double v: values ) {
		sum += v;
	    }
	}
	if( level<LEVELS ) {
	    level++;
	    master.submit( this, this );
	}
	else {
	    long id = 0l;
	    JobProgressValue result = new DoubleResultValue( sum );
	    Job j = new ReportResultJob( id, result );
	    master.submit( this, j );
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

    /**
     * Returns the type of this job.
     * @return The type of this job.
     */
    @Override
    public JobType getType() {
	return jobType;
    }
}
