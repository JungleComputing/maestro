package ibis.maestro;

/** A Maestro job that multiplies the array of values it is given. */
public class AdditionJob implements Job {
    private static final int BLOCK_SIZE = 1000;
    private static final int ITERATIONS = 200;  // The number of times we should do the addition.
    private static final int LEVELS = 2;
    private static int level = 0;
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
     * @return The result of this run.
     */
    @Override
    public DoubleReturnValue run( Master master )
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
	return new DoubleReturnValue( sum );
    }

    /**
     * Compare this job instance to another one.
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo( Job o) {
	if( o instanceof AdditionJob ) {
	    AdditionJob other = (AdditionJob) o;
	    return this.values.length-other.values.length;
	}
	return 0;
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

}
