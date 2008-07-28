package ibis.maestro;

import java.io.Serializable;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    private static final int ITERATIONS = 200;  // The number of times we should do the addition.
    private static final int ARRAY_SIZE = 5000;

    static final int LEVELS = 4;

    private static class Listener implements CompletionListener
    {
	int jobsCompleted = 0;
	private final int jobCount;

	Listener( int jobCount )
	{
	    this.jobCount = jobCount;
	}

	/** Handle the completion of task 'j': the result is 'result'.
	 * @param id The task that was completed.
	 * @param result The result of the task.
	 */
	@Override
	public void jobCompleted( Node node, Object id, Object result ) {
	    //System.out.println( "result is " + result );
	    jobsCompleted++;
            if( Settings.traceNodes ){
                System.out.println( "I now have " + jobsCompleted + "/" + jobCount + " jobs" );
            }
	    if( jobsCompleted>=jobCount ){
		System.out.println( "I got all task results back; stopping test program" );
		node.setStopped();
	    }
	}
    }

    private static class AdditionData implements Serializable {
	private static final long serialVersionUID = 1673728176628719415L;
	final double data[];

	private AdditionData(final double[] data) {
	    this.data = data;
	}

	/**
	 * Returns a string representation of this multiply task.
	 * @return The string representation.
	 */
	@Override
	public String toString()
	{
	    if( data.length == 0 ){
		return "(AdditionData [<empty>])";
	    }
	    return "(AdditionData [" + data[0] + ",...," + data[data.length-1] + "])";
	}

    }

    private static class CreateArrayTask implements AtomicTask
    {
	private static final long serialVersionUID = 2347248108353357517L;

	/**
	 * Returns the name of this task.
	 * @return The name.
	 */
	@Override
	public String getName()
	{
	    return "Create array";
	}

	/**
	 * Runs this task.
	 * @param obj The input parameter of this task.
	 * @param node The node the task is running on.
	 * @return The result value of this task.
	 */
	@Override
	@SuppressWarnings("synthetic-access")
	public AdditionData run( Object obj, Node node )
	{
	    Integer val = (Integer) obj;
	    double a[] = new double [ARRAY_SIZE];
	    for( int i=0; i<ARRAY_SIZE; i++ ) {
		a[i] = i+val;
	    }
	    return new AdditionData( a );
	}

	/**
	 * Returns true iff this task is supported in this context.
	 * @return True iff this task is supported.
	 */
	@Override
	public boolean isSupported()
	{
	    return true;
	}
    }

    private static class AdditionTask implements AtomicTask
    {
	private static final long serialVersionUID = 7652370809998864296L;


	/**
	 * Returns the name of this task.
	 * @return The name.
	 */
	@Override
	public String getName()
	{
	    return "Addition";
	}

	/**
	 * @param obj The input parameter of the task.
	 * @param node The node the task is running on.
	 * @return The result of the task.
	 */
	@Override
	public AdditionData run( Object obj, Node node )
	{
	    AdditionData data = (AdditionData) obj;
	    double sum = 0.0;
	    for( int i=0; i<ITERATIONS; i++ ) {
		for( double v: data.data ) {
		    sum += v;
		}
	    }
	    return data;
	}

	/**
	 * Returns true iff this task is supported in this context.
	 * @return True iff this task is supported.
	 */
	@Override
	public boolean isSupported()
	{
	    return true;
	}
    }

    private class AssembleArrayTask implements MapReduceTask {
	private static final long serialVersionUID = 1L;
	private final Job createJob;
	private static final int SIZE = 4;
	Object res[] = new Object[SIZE];

	AssembleArrayTask( Job job )
	{
	    this.createJob = job;
	}

	/**
	 * Returns the result of this map/reduce.
	 * @return The result of this map/reduce.
	 */
        @Override
	public Object getResult()
	{
	    // TODO: do something more interesting.
	    return res[0];
	}

        /**
         * Generate jobs to compute different components for this task. (Overrides method in superclass.)
         * @param input The input
         * @param handler The handler for this map/reduce task
         */
        @Override
	public void map( Object input, MapReduceHandler handler )
	{
	    for( int n=0; n<SIZE; n++ ){
		Integer userId = n;
		handler.submit( createJob, input, userId );
	    }
	}

	/**
	 * Add a given result to the collected result. (Overrides method in superclass.)
	 * @param id The identifier of the result.
	 * @param result The result.
	 */
	@Override
	public void reduce( Object id, Object result )
	{
	    Integer ix = (Integer) id;

	    res[ix] = result;
	}

	/**
	 * Returns the name of this task. (Overrides method in superclass.)
	 * @return The name.
	 */
        @Override
	public String getName()
	{
	    return "Assemble array";
	}

        /**
         * Is this task supported on this node?
         * @return <true> since all nodes support this task.
         */
        @Override
	public boolean isSupported() {
	    return true;
	}

    }

    @SuppressWarnings("synthetic-access")
    private void run( int taskCount, boolean goForMaestro ) throws Exception
    {
	Listener listener = new Listener( taskCount );
	JobList jobs = new JobList();

	Job createJob = jobs.createJob("createarray", new CreateArrayTask() );
	Job job = jobs.createJob(
		"testprog",
		new AssembleArrayTask( createJob ),
                //new CreateArrayTask(),
		new AdditionTask(),
		new AdditionTask(),
		new AdditionTask(),
		new AdditionTask()
	);
	Node node = new Node( jobs, goForMaestro );
	System.out.println( "Node created" );
	long startTime = System.nanoTime();
	if( node.isMaestro() ) {
	    System.out.println( "I am maestro; submitting " + taskCount + " tasks" );
	    for( int i=0; i<taskCount; i++ ){
		Integer length = 12*i;
		job.submit( node, length, i, listener );
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
	int taskCount = 0;

	if( args.length == 0 ){
	    System.err.println( "Missing parameter: I need a task count, or 'worker'" );
	    System.exit( 1 );
	}
	String arg = args[0];
	if( arg.equalsIgnoreCase( "worker" ) ){
	    goForMaestro = false;
	}
	else {
	    taskCount = Integer.parseInt( arg );
	}
	System.out.println( "Running on platform " + Service.getPlatformVersion() + " args.length=" + args.length + " goForMaestro=" + goForMaestro + "; taskCount=" + taskCount );
	try {
	    new TestProg().run( taskCount, goForMaestro );
	}
	catch( Exception e ) {
	    e.printStackTrace( System.err );
	}
    }
}
