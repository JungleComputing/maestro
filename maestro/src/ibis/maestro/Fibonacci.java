package ibis.maestro;

import java.io.Serializable;

class Fibonacci implements ParallelJob {
    static Fibonacci jobType;

    static class FibonacciInstance extends ParallelJobInstance {
        boolean haveResult0 = false;
        boolean haveResult1 = false;
        private int result;
        private int resultCount = 0;
        private int expectedResults;

        public FibonacciInstance(ParallelJobContext context) {
            super(context);
        }


        @Override
        public void split(Serializable input, ParallelJobHandler handler) {
            final int i = (Integer) input;

            if( i<3 ) {
                result = 1;
                expectedResults = 0;
            }
            else {
                result = 0;
                expectedResults = 2;
                handler.submit( i-1, this, 0, jobType );
                handler.submit( i-2, this, 1, jobType );
            }
        }

        @Override
        public void merge(Serializable idObject, Serializable v) {
            final int id = (Integer) idObject;
            final int n = (Integer) v;

            if( id == 0 ){
                if( !haveResult0 ){
                    result += n;
                    haveResult0 = true;
                    resultCount++;
                }
            }
            else if( id == 1 ){
                result += n;
                haveResult1 = true;
                resultCount++;
            }
            else {
                Globals.log.reportInternalError( "Bad id " + id );
            }
        }


        @Override
        public boolean resultIsReady()
        {
            return resultCount>=expectedResults;
        }


        @Override
        public Serializable getResult() {
            return result;
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public ParallelJobInstance createInstance(ParallelJobContext context) {
        return new FibonacciInstance(context);
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
        public void jobCompleted(Node node, Object id, Serializable result) {
            if (Settings.traceNodes) {
                System.out.println("I now have the result: " + result);
            }
            node.setStopped();
        }
    }

    @SuppressWarnings("synthetic-access")
    private static void run(int value, boolean goForMaestro) throws Exception {
        final JobList jobs = new JobList();

        jobType = new Fibonacci();
        jobs.registerJob( jobType );
        final Node node = Node.createNode(jobs, goForMaestro);
        final Listener listener = new Listener();
        System.out.println("Node created");
        final double startTime = Utils.getPreciseTime();
        if (node.isMaestro()) {
            System.out.println("I am maestro; submitting value " + value );
            node.submit(value, 0, listener, jobType);
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
