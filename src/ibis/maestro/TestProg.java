package ibis.maestro;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestProg {
    private static final int ITERATIONS = 800;  // The number of times we should do the addition.

    static final int LEVELS = 4;

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
        public void jobCompleted( Node node, Object id, Object result ) {
            //System.out.println( "result is " + result );
            jobsCompleted++;
            //System.out.println( "I now have " + jobsCompleted + "/" + jobCount + " jobs" );
            if( jobsCompleted>=jobCount ){
                System.out.println( "I got all job results back; stopping test program" );
                node.setStopped();
            }
        }
    }

    private static class AdditionData {
        final double data[];

        private AdditionData(final double[] data) {
            this.data = data;
        }

        /**
         * Returns a string representation of this multiply job.
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

    private static class CreateArrayJob implements Job
    {
        private static final long serialVersionUID = 2347248108353357517L;

        @SuppressWarnings("synthetic-access")
        public AdditionData run(Object obj, Node node )
        {
            Integer length = (Integer) obj;
            double a[] = new double [length];
            for( int i=0; i<length; i++ ) {
                a[i] = i+42;
            }
            return new AdditionData( a );
        }
    }

    private static class AdditionJob implements Job
    {
        private static final long serialVersionUID = 7652370809998864296L;

        /**
         * @param obj
         * @param node
         * @param taskId
         * @return
         */
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
    }

    @SuppressWarnings("synthetic-access")
    private void run( int jobCount, boolean goForMaestro ) throws Exception
    {
        Node node = new Node( goForMaestro );
        Listener listener = new Listener( jobCount );

        Task task = node.createTask( "testprog", new CreateArrayJob(), new AdditionJob(), new AdditionJob(), new AdditionJob(), new AdditionJob() );
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
            new TestProg().run( jobCount, goForMaestro );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
