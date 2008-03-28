package ibis.maestro;

/**
 * Small test program with a chain of 4 jobs that are each twice as
 * fast to compute as the previous one.
 * @author Kees van Reeuwijk
 *
 */
public class SlowToFastJobs {
    private static final int ITERATIONS = 1000;
    private static final int LEVELS = 4;

    /** A Maestro job that multiplies the array of values it is given. */
    public static class ReversalJob implements Job {
	/** Contractual obligation. */
	private static final long serialVersionUID = 132L;
	private final double values[];
	private int level;

	ReversalJob( double values[], int stage )
	{
	    this.values = values;
	    this.level = stage;
	}

	/**
	 * Runs this job.
	 */
	@Override
	public void run( Node node, TaskIdentifier taskid )
	{
	    for( int i=0; i<ITERATIONS; i++ ) {
		int startIx = 0;
		int endIx = values.length-1;
		while( startIx<endIx ) {
		    double tmp = values[endIx];
		    values[endIx] = values[startIx];
		    values[startIx] = tmp;
		    startIx++;
		    endIx--;
		}
	    }
	    if( level<LEVELS ) {
		double values1[] = new double[(1+values.length)/2];
		for( int ix=0; ix<values1.length; ix++ ) {
		    values1[ix] = values[ix*2] + values[ix*2+1];
		}
		Job job = new ReversalJob( values1, level+1 );
		node.submit( job, taskid );
	    }
	    else {
		double sum = 0.0;
		
		for( double v: values ) {
		    sum += v;
		}
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
	    return "ReversalJob stage " + level;
	}

	static JobType buildJobType( int level )
	{
	    return new JobType( level, "ReversalJob" );
	}
	/**
	 * Returns the type of this job.
	 * @return The type of this job.
	 */

	@Override
	public JobType getType() {
	    return buildJobType( level );
	}
    }

    private static class Listener implements CompletionListener
    {
	int jobsCompleted = 0;
	private final int jobCount;

	Listener( int jobCount )
	{
	    this.jobCount = jobCount;
	}

	/** Handle the completion of job 'j': the result is 'result'.
	 * @param id The job that was completed.
	 * @param result The result of the job.
	 */
	@Override
	public void jobCompleted( Node node, TaskIdentifier id, JobResultValue result ) {
	    //System.out.println( "result is " + result );
	    jobsCompleted++;
	    //System.out.println( "I now have " + jobsCompleted + "/" + jobCount + " jobs" );
	    if( jobsCompleted>=jobCount ){
		System.out.println( "I got all job results back; stopping test program" );
		node.setStopped();
	    }
	}
    }

    private static final class TestTypeInformation implements TypeInformation {

	/**
	 * Registers that a neighbor supports the given type of job.
	 * @param w The worker to register the info with.
	 * @param t The type a neighbor supports.
	 */
	@Override
	public void registerNeighborType( Node w, JobType t )
	{
	    // Nothing to do.
	}

	/** Registers the initial types of this worker.
	 * 
	 * @param w The worker to initialize.
	 */
	@Override
	public void initialize( Node w)
	{
	    for( int stage=0; stage<LEVELS; stage++ ) {
		w.allowJobType( ReversalJob.buildJobType( stage ) );
	    }
	}

	/**
	 * Compares two job types based on priority. Returns
	 * 1 if type a has more priority as b, etc.
	 * @param a One of the job types to compare.
	 * @param b The other job type to compare.
	 * @return The comparison result.
	 */
	public int compare( JobType a, JobType b )
	{
	    return JobType.comparePriorities( a, b);
	}

    }

    @SuppressWarnings("synthetic-access")
    private void run( int jobCount, boolean goForMaestro ) throws Exception
    {
	Node node = new Node( new TestTypeInformation(), goForMaestro );
	Listener listener = new Listener( jobCount );

	System.out.println( "Node created" );
	if( node.isMaestro() ) {
	    System.out.println( "I am maestro; submitting " + jobCount + " jobs" );
	    for( int i=0; i<jobCount; i++ ){
		TaskIdentifier id = node.buildTaskIdentifier( i );
		AdditionJob j = new AdditionJob( 12*i );
		node.submitTaskWhenRoom( j, listener, id );
	    }
	}
	node.waitToTerminate();
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] )
    {
	boolean goForMaestro = true;
	int jobCount = 0;

	if( args.length == 0 ){
	    System.err.println( "Missing parameter: I need a job count, or 'worker'" );
	    System.exit( 1 );
	}
	String arg = args[0];
	if( arg.equalsIgnoreCase( "worker" ) ){
	    goForMaestro = false;
	}
	else {
	    jobCount = Integer.parseInt( arg );
	}
	System.out.println( "Running on platform " + Service.getPlatformVersion() + " args.length=" + args.length + " goForMaestro=" + goForMaestro + "; jobCount=" + jobCount );
	try {
	    new SlowToFastJobs().run( jobCount, goForMaestro );
	}
	catch( Exception e ) {
	    e.printStackTrace( System.err );
	}
    }
}
