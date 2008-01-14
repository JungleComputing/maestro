package ibis.maestro;

/** A Maestro job that multiplies the array of values it is given. */
public class MultiplyJob implements Job {
    private final double values[];
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    MultiplyJob( double values[] )
    {
	this.values = values;
    }

    /**
     * Runs this job.
     * @return The result of this run.
     */
    @Override
    public DoubleReturnValue run() {
	double res = 1;
	
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
	String s = "[";
	for( double v: values ) {
	    s += v + ",";
	}
	return "(MultiplyJob " + s + "])";
    }

}
