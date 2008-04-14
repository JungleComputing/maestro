package ibis.maestro;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
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
        private static final long serialVersionUID = -4358159127247563219L;

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
        public void initialize( Node w )
        {
            for( int level=0; level<=AdditionJob.LEVELS; level++ ) {
        	w.allowJobType( AdditionJob.buildJobType( level ) );
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
        long startTime = System.nanoTime();
        if( node.isMaestro() ) {
            System.out.println( "I am maestro; submitting " + jobCount + " jobs" );
            for( int i=0; i<jobCount; i++ ){
                TaskIdentifier id = node.buildTaskIdentifier( i );
                AdditionJob j = new AdditionJob( 12*i );
                node.submitTaskWhenRoom( j, listener, id );
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
            new TestProg().run( jobCount, goForMaestro );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
