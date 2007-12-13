package ibis.maestro;

/** A Maestro job that multiplies the array of values it is given. */
public class MultiplyJob implements Job {
    private final double values[];
    
    class Result implements JobResult {
	private final double value;
	
	Result( double v )
	{
	    value = v;
	}
	
	double getValue()
	{
	    return value;
	}
    }

    MultiplyJob( double values[] )
    {
	this.values = values;
    }

    /**
     * Runs this job.
     * @return The result of this run.
     */
    @Override
    public JobResult run() {
	double res = 1;
	
	for( double v: values ) {
	    res *= v;
	}
	return new Result( res );
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

}
