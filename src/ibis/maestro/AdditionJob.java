package ibis.maestro;

/** A Maestro job that multiplies the array of values it is given. */
public class AdditionJob implements Job {
    private static final int BLOCK_SIZE = 4000;
    private static final int ITERATIONS = 5000;  // The number of times we should do the addition.
    private static final int LEVELS = 4;
    private int level = 0;
    private static double sum = 0.0;
    private final double values[];
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    static final JobType jobType = new JobType( "AdditionJob" );
    private final Identifier id;

    private static class Identifier implements TaskIdentifier {
        final int n;
        
        Identifier( int n ){
            this.n = n;
        }
    }

    AdditionJob( int n )
    {
	double a[] = new double [BLOCK_SIZE];
	for( int i=0; i<BLOCK_SIZE; i++ ) {
	    a[i] = i+n;
	}
	this.values = a;
	this.id = new Identifier( n );
    }

    /**
     * Runs this job.
     */
    @Override
    public void run( Node master )
    {
	for( int i=0; i<ITERATIONS; i++ ) {
	    for( double v: values ) {
		sum += v;
	    }
	}
	if( level<LEVELS ) {
	    level++;
	    master.submit( this );
	}
	else {
	    JobResultValue result = new DoubleResultValue( sum );
	    Job j = new ReportResultJob( id, result );
	    master.submit( j );
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
