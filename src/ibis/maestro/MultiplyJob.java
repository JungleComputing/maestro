package ibis.maestro;

/** A Maestro job that multiplies the array of values it is given. */
public class MultiplyJob implements Job {
    private static final int BLOCK_SIZE = 1000;
    private final double values[];
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private static final JobType jobType = new JobType( "MultiplyJob" );

    MultiplyJob( int n )
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
	double res = 1;
	
	for( double v: values ) {
	    res *= v;
	}
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
        for( double v: values ) {
            res *= v;
        }
	return new DoubleReturnValue( res );
    }

    /**
     * Compare this job instance to another one.
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo( Job o) {
	if( o instanceof MultiplyJob ) {
	    MultiplyJob other = (MultiplyJob) o;
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
	return "(MultiplyJob [" + values[0] + ",...," + values[values.length-1] + "])";
    }

    /**
     * @return The type of this job.
     */
    @Override
    public JobType getType() {
	return jobType;
    }
}
