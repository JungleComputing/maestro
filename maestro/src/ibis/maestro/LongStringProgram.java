package ibis.maestro;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.Random;


/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
class LongStringProgram
{
    private static final int K = 1000;
    private static final int MINIMAL_STRING_SIZE = 10*K;
    private static final int MAXIMAL_STRING_SIZE = 300*K;

    private static final Random rng = new Random();

    private static class Result implements Serializable
    {
	private static final long serialVersionUID = 1L;
	final String s;
	final int len;
	final int checksum;

	static int calculateChecksum( String s )
	{
	    int sz = s.length();
	    int sum = 0;

	    for( int i=0; i<sz; i++ ) {
		sum += s.charAt( i );
	    }
	    return sum;
	}

	Result( String s )
	{
	    this.s = s;
	    this.len = s.length();
	    this.checksum = calculateChecksum( s );
	}
	
	void check()
	{
	    if( s == null ) {
		System.err.println( "Null string" );
		return;
	    }
	    if( s.length() != len ) {
		System.err.println( "Recorded length is " + len + " actual length is " + s.length() );
		return;
	    }
	    int localChecksum = calculateChecksum( s );
	    if( checksum != localChecksum ) {
		System.err.println( "Recorded checksum " + checksum + " from real checksum " + localChecksum );
	    }
	}
    }

    private static class Listener implements JobCompletionListener
    {
        int jobsCompleted = 0;
        private final int jobCount;

        Listener( Node node, int jobCount )
        {
            this.jobCount = jobCount;
            if( jobCount == 0 ) {
                node.setStopped();
            }
        }

        /** Handle the completion of task 'j': the result is 'result'.
         * @param id The task that was completed.
         * @param result The result of the task.
         */
        @Override
        public void jobCompleted( Node node, Object id, Object result ) {
            //System.out.println( "result is " + result );
            Result res = (Result) result;
            res.check();
            jobsCompleted++;
            System.out.println( "I now have " + jobsCompleted + "/" + jobCount + " jobs" );
            if( jobsCompleted>=jobCount ){
                System.out.println( "I got all task results back; stopping test program" );
                node.setStopped();
            }
        }
    }

    private static class BuildStringTask implements UnpredictableAtomicTask
    {
        private static final long serialVersionUID = 7652370809998864296L;

        private static Result buildRandomString( int sz )
        {
            byte buf[] = new byte[sz];
            for( int i=0; i<sz; i++ ) {
        	buf[i] = ((byte) (' ' + rng.nextInt( '~'-' ' )));
            }
            return new Result( new String( buf ) );
        }

        private long runBenchmark()
        {
            long startTime = System.nanoTime();
            buildRandomString( (MINIMAL_STRING_SIZE+MAXIMAL_STRING_SIZE)/2 );
            return System.nanoTime()-startTime;
        }

        /** Estimate the time to compare two files. (Overrides method in superclass.)
         * We try to get an estimate that is representative of the processor, so that
         * we pick an efficient processor first, but lower than the real execution time,
         * so that the system is encouraged to try all processors (and at least initially
         * spread the load).
         * @return The estimated execution time of a task.
         */
        @Override
        public long estimateTaskExecutionTime()
        {
            return runBenchmark();
        }


        /**
         * Returns the name of this task.
         * @return The name.
         */
        @Override
        public String getName()
        {
            return "Sharpen";
        }

        /**
         * @param obj The input parameter of the task.
         * @param node The node the task is running on.
         * @return The result of the task.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public Object run( Object obj, Node node )
        {
            int sz = (Integer) obj;
            return buildRandomString( sz );
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

    @SuppressWarnings("synthetic-access")
    private void run( int taskCount, boolean goForMaestro, int waitNodes ) throws Exception
    {
        JobList jobs = new JobList();

        Job job = jobs.createJob(
            "sharpenprog",
            new BuildStringTask()
        );
        Node node = new Node( jobs, goForMaestro );
        Listener listener = new Listener( node, taskCount );
        System.out.println( "Node created" );
        long startTime = System.nanoTime();
        if( node.isMaestro() ) {
            boolean goodToSubmit = true;
            if( waitNodes>0 ) {
                System.out.println( "Waiting for " + waitNodes + " ready nodes" );
                int n = node.waitForReadyNodes( waitNodes, 3*60*1000 ); // Wait for maximally 3 minutes for this many nodes.
                System.out.println( "There are now " + n + " nodes available" );
                if( n*3<waitNodes ) {
                    System.out.println( "That is less than a third of the required nodes; goodbye!");
                    goodToSubmit = false;
                }
            }
            if( goodToSubmit ) {
                System.out.println( "I am maestro; submitting " + taskCount + " tasks" );
                for( int i=0; i<taskCount; i++ ){
                    Integer length = 12*i;
                    job.submit( node, length, i, listener );
                }
            }
            else {
                node.setStopped();
            }
        }
        node.waitToTerminate();
        long stopTime = System.nanoTime();
        System.out.println( "Duration of this run: " + Utils.formatNanoseconds( stopTime-startTime ) );
    }

    private static void usage( PrintStream printStream )
    {
        printStream.println( "Usage: LongStringProgram [<options>] <jobCount>" );
        printStream.println( " empty <jobCount> for a worker" );
        printStream.println( " -h      Show this help" );
        printStream.println( " -w <n>  Wait for at least <n> ready nodes before submitting jobs" );
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] )
    {
        boolean goForMaestro = false;
        int taskCount = 0;
        int waitNodes = 0;

        for (int i=0;i<args.length;i++) { 

            if (args[i].equals("-h") || args[i].equals("--help")) { 
                usage( System.out );
                System.exit( 0 );
            }
            else if (args[i].equals("-w") || args[i].equals("--waitnodes")) {
                waitNodes = Integer.parseInt(  args[++i] );
            }
            else {
                taskCount = Integer.parseInt( args[i] );
                goForMaestro = true;
            }
        }
        System.out.println( "Running on platform " + Utils.getPlatformVersion() + " args.length=" + args.length + " goForMaestro=" + goForMaestro + "; taskCount=" + taskCount );
        try {
            new LongStringProgram().run( taskCount, goForMaestro, waitNodes );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
