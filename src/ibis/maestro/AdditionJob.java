package ibis.maestro;

/** A Maestro job that multiplies the array of values it is given. */
public class AdditionJob implements Job {
    private static final int BLOCK_SIZE = 3000;
    private static final int ITERATIONS = 10000;  // The number of times we should do the addition.
    private static final int LEVELS = 3;
    private int level = 0;
    private static double sum = 0.0;
    private final double values[];
    private final ReportReceiver watcher;
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    static final JobType jobType = new JobType( "AdditionJob" );

    AdditionJob( int n, ReportReceiver watcher )
    {
	double a[] = new double [BLOCK_SIZE];
	
	this.watcher = watcher;
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
	    master.reportResult( watcher, new DoubleResultValue( sum ) );
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
