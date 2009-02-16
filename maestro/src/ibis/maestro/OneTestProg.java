package ibis.maestro;

import java.io.Serializable;

/**
 * Small test program.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class OneTestProg {
    private static final int ITERATIONS = 8000;

    private static final int ARRAY_SIZE = 100000;

    private static class Listener implements JobCompletionListener {
        int jobsCompleted = 0;

        private final int jobCount;

        Listener(int jobCount) {
            this.jobCount = jobCount;
        }

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
            // System.out.println( "result is " + result );
            jobsCompleted++;
            // System.out.println( "I now have " + jobsCompleted + "/" +
            // jobCount + " jobs" );
            if (jobsCompleted >= jobCount) {
                System.out
                        .println("I got all job results back; stopping test program");
                node.setStopped();
            }
        }
    }

    private static final class Empty implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private static class CreateArrayJob implements AtomicJob {
        private static final long serialVersionUID = 2347248108353357517L;

        /**
         * Runs this job.
         * 
         * @param obj
         *            The input parameter of this job.
         * @return The result value of this job.
         */
        @Override
        @SuppressWarnings("synthetic-access")
        public Object run(Object obj) {
            int val = (Integer) obj;
            double a[] = new double[ARRAY_SIZE];
            for (int n = 0; n < ITERATIONS; n++) {
                for (int i = 0; i < ARRAY_SIZE; i++) {
                    a[i] = i + val;
                }
            }
            return new Empty();
        }

        /**
         * Returns true iff this job is supported in this context.
         * 
         * @return True iff this job is supported.
         */
        @Override
        public boolean isSupported() {
            return true;
        }
    }

    @SuppressWarnings("synthetic-access")
    private void run(int jobCount, boolean goForMaestro) throws Exception {
        Listener listener = new Listener(jobCount);
        JobList jobs = new JobList();

        JobSequence job = jobs.createJobSequence( new CreateArrayJob());
        Node node = Node.createNode(jobs, goForMaestro);
        System.out.println("Node created");
        double startTime = Utils.getPreciseTime();
        if (node.isMaestro()) {
            System.out.println("I am maestro; submitting " + jobCount
                    + " jobs");
            for (int i = 0; i < jobCount; i++) {
                Integer length = 12 * i;
                node.submit(length, i, listener, job);
            }
        }
        node.waitToTerminate();
        double stopTime = Utils.getPreciseTime();
        System.out.println("Duration of this run: "
                + Utils.formatSeconds(stopTime - startTime));
    }

    /**
     * The command-line interface of this program.
     * 
     * @param args
     *            The list of command-line parameters.
     */
    public static void main(String args[]) {
        boolean goForMaestro = true;
        int jobCount = 0;

        if (args.length == 0) {
            System.err
                    .println("Missing parameter: I need a task count, or 'worker'");
            System.exit(1);
        }
        String arg = args[0];
        if (arg.equalsIgnoreCase("worker")) {
            goForMaestro = false;
        } else {
            jobCount = Integer.parseInt(arg);
        }
        System.out.println("Running on platform " + Utils.getPlatformVersion()
                + " args.length=" + args.length + " goForMaestro="
                + goForMaestro + "; jobCount=" + jobCount);
        try {
            new OneTestProg().run(jobCount, goForMaestro);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
