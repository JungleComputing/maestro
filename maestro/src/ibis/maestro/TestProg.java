package ibis.maestro;

import java.io.Serializable;

/**
 * Small test program.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class TestProg {
    private static final int ITERATIONS = 200;

    private static final int ARRAY_SIZE = 5000;

    static final int LEVELS = 4;

    private static class Listener implements JobCompletionListener {
        int jobsCompleted = 0;

        private final int jobCount;

        Listener(Node node, int jobCount) {
            this.jobCount = jobCount;
            if (jobCount == 0) {
                node.setStopped();
            }
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
        public void jobCompleted(Node node, Object id, Serializable result) {
            // System.out.println( "result is " + result );
            jobsCompleted++;
            if (Settings.traceNodes) {
                System.out.println("I now have " + jobsCompleted + "/"
                        + jobCount + " jobs");
            }
            if (jobsCompleted >= jobCount) {
                System.out
                        .println("I got all job results back; stopping test program");
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
         * Returns a string representation of this multiply job.
         * 
         * @return The string representation.
         */
        @Override
        public String toString() {
            if (data.length == 0) {
                return "(AdditionData [<empty>])";
            }
            return "(AdditionData [" + data[0] + ",...,"
                    + data[data.length - 1] + "])";
        }

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
        public AdditionData run(Serializable obj) {
            final Integer val = (Integer) obj;
            final double a[] = new double[ARRAY_SIZE];

            for (int i = 0; i < ARRAY_SIZE; i++) {
                a[i] = i + val;
            }
            return new AdditionData(a);
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

    private static class AdditionJob implements AtomicJob {
        private static final long serialVersionUID = 7652370809998864296L;

        /**
         * @param obj
         *            The input parameter of the job.
         * @return The result of the job.
         */
        @Override
        public AdditionData run(Serializable obj) {
            final AdditionData data = (AdditionData) obj;
            double sum = 0.0;
            for (int i = 0; i < ITERATIONS; i++) {
                for (final double v : data.data) {
                    sum += v;
                }
            }
            return data;
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
    private static void run(int jobCount, boolean goForMaestro)
            throws Exception {
        final JobList jobs = new JobList();

        // createJob = jobs.createJob("createarray", new CreateArrayJob()
        // );
        final SeriesJob job = new SeriesJob(
                // new AssembleArrayJob( createJob ),
                new CreateArrayJob(), new AdditionJob(), new AdditionJob(),
                new AdditionJob(), new AdditionJob());
        jobs.registerJob(job);
        final Node node = Node.createNode(jobs, goForMaestro);
        final Listener listener = new Listener(node, jobCount);
        System.out.println("Node created");
        final double startTime = Utils.getPreciseTime();
        if (node.isMaestro()) {
            System.out
                    .println("I am maestro; submitting " + jobCount + " jobs");
            for (int i = 0; i < jobCount; i++) {
                final Integer length = 12 * i;
                node.submit(length, i, listener, job);
            }
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
        int jobCount = 0;

        if (args.length == 0) {
            System.err
                    .println("Missing parameter: I need a job count, or 'worker'");
            System.exit(1);
        }
        final String arg = args[0];
        if (arg.equalsIgnoreCase("worker")) {
            goForMaestro = false;
        } else {
            jobCount = Integer.parseInt(arg);
        }
        System.out.println("Running on platform " + Utils.getPlatformVersion()
                + " args.length=" + args.length + " goForMaestro="
                + goForMaestro + "; jobCount=" + jobCount);
        try {
            run(jobCount, goForMaestro);
        } catch (final Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
