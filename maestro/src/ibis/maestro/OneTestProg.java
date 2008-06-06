package ibis.maestro;

import java.io.Serializable;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class OneTestProg {
    private static final int ITERATIONS = 8000;  // The number of times we should do the addition.
    private static final int ARRAY_SIZE = 100000;

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
	public void taskCompleted( Node node, Object id, Object result ) {
	    //System.out.println( "result is " + result );
	    jobsCompleted++;
	    //System.out.println( "I now have " + jobsCompleted + "/" + jobCount + " jobs" );
	    if( jobsCompleted>=jobCount ){
		System.out.println( "I got all job results back; stopping test program" );
		node.setStopped();
	    }
	}
    }

    private static final class Empty implements Serializable {
	private static final long serialVersionUID = 1L;
    }

    private static class CreateArrayJob implements Job
    {
	private static final long serialVersionUID = 2347248108353357517L;

	/**
	 * Runs this job.
	 * @param obj The input parameter of this job.
	 * @param node The node the job is running on.
	 * @param context The program-specific context of this node.
	 * @return The result value of this job.
	 */
	@Override
	@SuppressWarnings("synthetic-access")
	public Object run( Object obj, Node node )
	{
	    int val = (Integer) obj;
	    double a[] = new double [ARRAY_SIZE];
	    for( int n=0; n<ITERATIONS; n++ ) {
		for( int i=0; i<ARRAY_SIZE; i++ ) {
		    a[i] = i+val;
		}
	    }
	    return new Empty();
	}

	/**
	 * Returns true iff this job is supported in this context.
	 * @param context The program context.
	 * @return True iff this job is supported.
	 */
	@Override
	public boolean isSupported()
	{
	    return true;
	}
    }

    @SuppressWarnings("synthetic-access")
    private void run( int jobCount, boolean goForMaestro ) throws Exception
    {
	Node node = new Node( goForMaestro );
	Listener listener = new Listener( jobCount );

	Task task = node.createTask( "testprog", new CreateArrayJob() );
	System.out.println( "Node created" );
	long startTime = System.nanoTime();
	if( node.isMaestro() ) {
	    System.out.println( "I am maestro; submitting " + jobCount + " jobs" );
	    for( int i=0; i<jobCount; i++ ){
		Integer length = new Integer( 12*i );
		task.submit( length, i, listener );
	    }
	}
	node.waitToTerminate();
	long stopTime = System.nanoTime();
	System.out.println( "Duration of this run: " + Service.formatNanoseconds( stopTime-startTime ) );
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
	    new OneTestProg().run( jobCount, goForMaestro );
	}
	catch( Exception e ) {
	    e.printStackTrace( System.err );
	}
    }
}
