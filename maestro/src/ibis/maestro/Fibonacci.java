package ibis.maestro;

import java.io.Serializable;

class Fibonacci implements ParallelJob {
    static class FibonacciInstance implements ParallelJobInstance {
        boolean haveResult0 = false;
        boolean haveResult1 = false;
        private int result;
        private int results = 0;
        private final int expectedResults;

        public FibonacciInstance(int result, int expectedResults) {
            this.result = result;
            this.expectedResults = expectedResults;
        }

        @Override
        public Object getResult() {
            return result ;
        }

        @Override
        public boolean resultIsReady()
        {
            return results>=expectedResults;
        }

        @Override
        public void merge(Serializable idObject, Object v) {
            final int id = (Integer) idObject;
            final int n = (Integer) v;

            if( id == 0 ){
                if( !haveResult0 ){
                    result += n;
                    haveResult0 = true;
                    results++;
                }
            }
            else if( id == 1 ){
                result += n;
                haveResult1 = true;
                results++;
            }
            else {
                Globals.log.reportInternalError( "Bad id " + id );
            }
        }
    }

    @Override
    public ParallelJobInstance split(Object input, ParallelJobHandler handler) {
        final int i = (Integer) input;
        FibonacciInstance res;

        if( i<3 ) {
            res = new FibonacciInstance( 1, 0 );
        }
        else {
            res = new FibonacciInstance( 0, 2 );
            handler.submit( i-1, res, 0, this );
            handler.submit( i-2, res, 1, this );
        }
        return res;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    private static class Listener implements JobCompletionListener {

        /**
         * Handle the completion of job 'j': the result is 'result'.
         * 
         * @param id
         *            The job that was completed.
         * @param result
         *            The result of the job.
         */
        @Override
        public void jobCompleted(Node node, Object id, Object result) {
            if (Settings.traceNodes) {
                System.out.println("I now have the result: " + result);
            }
            node.setStopped();
        }
    }

    @SuppressWarnings("synthetic-access")
    private static void run(int value, boolean goForMaestro) throws Exception {
        final JobList jobs = new JobList();

        final Job job = new Fibonacci();
        jobs.registerJob( job );
        final Node node = Node.createNode(jobs, goForMaestro);
        final Listener listener = new Listener();
        System.out.println("Node created");
        final double startTime = Utils.getPreciseTime();
        if (node.isMaestro()) {
            System.out.println("I am maestro; submitting value " + value );
            node.submit(value, 0, listener, job);
        }
        node.waitToTerminate();
        final double stopTime = Utils.getPreciseTime();
        System.out.println("DURATION " + (stopTime - startTime));
    }

    /**
     * The command-line interface of this program.
     * 
     * @param args
     *            The list of command-line parameters.
     */
    public static void main(String args[]) {
        boolean goForMaestro = true;
        int value = 0;

        if (args.length == 0) {
            System.err
            .println("Missing parameter: I need a value, or 'worker'");
            System.exit(1);
        }
        final String arg = args[0];
        if (arg.equalsIgnoreCase("worker")) {
            goForMaestro = false;
        } else {
            value = Integer.parseInt(arg);
        }
        System.out.println("Running on platform " + Utils.getPlatformVersion()
                + " args.length=" + args.length + " goForMaestro="
                + goForMaestro + "; jobCount=" + value);
        try {
            run(value, goForMaestro);
        } catch (final Exception e) {
            e.printStackTrace(System.err);
        }
    }

}
