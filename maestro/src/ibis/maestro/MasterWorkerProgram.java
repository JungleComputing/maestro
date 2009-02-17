package ibis.maestro;

import java.io.PrintStream;
import java.util.Random;

/**
 * Small test program.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class MasterWorkerProgram {
    private static final int MINIMAL_SHARPENS = 10;

    private static final int MAXIMAL_SHARPENS = 50;

    private static final Random rng = new Random();

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
         * Handle the completion of task 'j': the result is 'result'.
         * 
         * @param id
         *            The task that was completed.
         * @param result
         *            The result of the task.
         */
        @Override
        public void jobCompleted(Node node, Object id, Object result) {
            // System.out.println( "result is " + result );
            jobsCompleted++;
            System.out.println("I now have " + jobsCompleted + "/" + jobCount
                    + " jobs");
            if (jobsCompleted >= jobCount) {
                System.out
                        .println("I got all task results back; stopping test program");
                node.setStopped();
            }
        }
    }

    private static class SharpenJob implements UnpredictableAtomicJob {
        private static final long serialVersionUID = 7652370809998864296L;

        private static final int BANDS = 3;

        private static final int width = 1000;

        private static final int height = 1000;

        /**
         * Given a byte returns an unsigned int.
         * 
         * @param v
         * @return
         */
        private static int byteToInt(byte v) {
            return v & 0xFF;
        }

        private static byte applyConvolution(byte v00, byte v01, byte v02,
                byte v10, byte v11, byte v12, byte v20, byte v21, byte v22,
                int k00, int k01, int k02, int k10, int k11, int k12, int k20,
                int k21, int k22, int weight) {
            int val = k00 * byteToInt(v00) + k10 * byteToInt(v10) + k20
                    * byteToInt(v20) + k01 * byteToInt(v01) + k11
                    * byteToInt(v11) + k21 * byteToInt(v21) + k02
                    * byteToInt(v02) + k12 * byteToInt(v12) + k22
                    * byteToInt(v22);
            return (byte) Math.min(255, Math
                    .max(0, (val + weight / 2) / weight));
        }

        /**
         * Applies the kernel with the given factors to an image, and returns a
         * new image.
         * 
         * @return The image with the kernel applied.
         */
        private static long convolution3x3(int k00, int k01, int k02, int k10,
                int k11, int k12, int k20, int k21, int k22, int weight) {
            byte res[] = new byte[width * height * BANDS];
            byte data[] = new byte[width * height * BANDS];
            final int rowBytes = width * BANDS;

            // Copy the top and bottom line into the result image.
            System.arraycopy(data, 0, res, 0, rowBytes);
            System.arraycopy(data, (height - 1) * rowBytes, res, (height - 1)
                    * rowBytes, rowBytes);

            /** Apply kernal to the remaining rows. */
            int wix = rowBytes; // Skip first row.
            byte r00, r01, r02, r10, r11, r12, r20, r21, r22;
            byte g00, g01, g02, g10, g11, g12, g20, g21, g22;
            byte b00, b01, b02, b10, b11, b12, b20, b21, b22;
            for (int h = 1; h < (height - 1); h++) {
                int rix = rowBytes * h;
                r00 = data[rix - rowBytes];
                r01 = data[rix];
                r02 = data[rix + rowBytes];
                rix++;
                g00 = data[rix - rowBytes];
                g01 = data[rix];
                g02 = data[rix + rowBytes];
                rix++;
                b00 = data[rix - rowBytes];
                b01 = data[rix];
                b02 = data[rix + rowBytes];
                rix++;
                r10 = data[rix - rowBytes];
                r11 = data[rix];
                r12 = data[rix + rowBytes];
                rix++;
                g10 = data[rix - rowBytes];
                g11 = data[rix];
                g12 = data[rix + rowBytes];
                rix++;
                b10 = data[rix - rowBytes];
                b11 = data[rix];
                b12 = data[rix + rowBytes];
                rix++;
                // Write the left border pixel.
                res[wix++] = r00;
                res[wix++] = g00;
                res[wix++] = b00;
                for (int w = 1; w < (width - 1); w++) {
                    r20 = data[rix - rowBytes];
                    r21 = data[rix];
                    r22 = data[rix + rowBytes];
                    rix++;
                    g20 = data[rix - rowBytes];
                    g21 = data[rix];
                    g22 = data[rix + rowBytes];
                    rix++;
                    b20 = data[rix - rowBytes];
                    b21 = data[rix];
                    b22 = data[rix + rowBytes];
                    rix++;
                    res[wix++] = applyConvolution(r00, r01, r02, r10, r11, r12,
                            r20, r21, r22, k00, k01, k02, k10, k11, k12, k20,
                            k21, k22, weight);
                    res[wix++] = applyConvolution(g00, g01, g02, g10, g11, g12,
                            g20, g21, g22, k00, k01, k02, k10, k11, k12, k20,
                            k21, k22, weight);
                    res[wix++] = applyConvolution(b00, b01, b02, b10, b11, b12,
                            b20, b21, b22, k00, k01, k02, k10, k11, k12, k20,
                            k21, k22, weight);
                    r00 = r10;
                    r10 = r20;
                    r01 = r11;
                    r11 = r21;
                    r02 = r12;
                    r12 = r22;
                    g00 = g10;
                    g10 = g20;
                    g01 = g11;
                    g11 = g21;
                    g02 = g12;
                    g12 = g22;
                    b00 = b10;
                    b10 = b20;
                    b01 = b11;
                    b11 = b21;
                    b02 = b12;
                    b12 = b22;
                }
                // Write the right border pixel.
                res[wix++] = r11;
                res[wix++] = g11;
                res[wix++] = b11;
            }
            long sum = 0;

            for (byte b : res) {
                sum += b;
            }
            return sum;
        }

        private static long sharpen() {
            return convolution3x3(-1, -1, -1, -1, 9, -1, -1, -1, -1, 1);
        }

        private double runBenchmark() {
            double startTime = Utils.getPreciseTime();
            sharpen();
            return Utils.getPreciseTime() - startTime;
        }

        /**
         * Estimate the time to compare two files. (Overrides method in
         * superclass.) We try to get an estimate that is representative of the
         * processor, so that we pick an efficient processor first, but lower
         * than the real execution time, so that the system is encouraged to try
         * all processors (and at least initially spread the load).
         * 
         * @return The estimated execution time of a task.
         */
        @Override
        public double estimateJobExecutionTime() {
            double benchmarkTime = runBenchmark();
            return MINIMAL_SHARPENS * benchmarkTime;
        }

        /**
         * @param obj
         *            The input parameter of the task.
         * @return The result of the task.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public Object run(Object obj) {
            int n = MINIMAL_SHARPENS
                    + rng.nextInt(MAXIMAL_SHARPENS - MINIMAL_SHARPENS);
            long sum = 0;

            for (int i = 0; i < n; i++) {
                sum += sharpen();
            }
            return sum;
        }

        /**
         * Returns true iff this task is supported in this context.
         * 
         * @return True iff this task is supported.
         */
        @Override
        public boolean isSupported() {
            return true;
        }
    }

    @SuppressWarnings("synthetic-access")
    private void run(int taskCount, boolean goForMaestro, int waitNodes)
            throws Exception {
        JobList jobs = new JobList();

        JobSequence job = jobs.createJobSequence( new SharpenJob());
        Node node = Node.createNode(jobs, goForMaestro);
        Listener listener = new Listener(node, taskCount);
        System.out.println("Node created");
        double startTime = Utils.getPreciseTime();
        if (node.isMaestro()) {
            boolean goodToSubmit = true;
            if (waitNodes > 0) {
                System.out.println("Waiting for " + waitNodes + " ready nodes");
                int n = node.waitForReadyNodes(waitNodes, 3 * 60 * 1000);
                // Wait for maximally 3 minutes for this many nodes.
                System.out.println("There are now " + n + " nodes available");
                if (n * 3 < waitNodes) {
                    System.out
                            .println("That is less than a third of the required nodes; goodbye!");
                    goodToSubmit = false;
                }
            }
            if (goodToSubmit) {
                System.out.println("I am maestro; submitting " + taskCount
                        + " tasks");
                for (int i = 0; i < taskCount; i++) {
                    Integer length = 12 * i;
                    node.submit(length, i, listener, job);
                }
            } else {
                node.setStopped();
            }
        }
        node.waitToTerminate();
        double stopTime = Utils.getPreciseTime();
        System.out.println("Duration of this run: "
                + Utils.formatSeconds(stopTime - startTime));
    }

    private static void usage(PrintStream printStream) {
        printStream
                .println("Usage: MasterWorkerProgram [<options>] <jobCount>");
        printStream.println(" empty <jobCount> for a worker");
        printStream.println(" -h      Show this help");
        printStream
                .println(" -w <n>  Wait for at least <n> ready nodes before submitting jobs");
    }

    /**
     * The command-line interface of this program.
     * 
     * @param args
     *            The list of command-line parameters.
     */
    public static void main(String args[]) {
        boolean goForMaestro = false;
        int taskCount = 0;
        int waitNodes = 0;

        for (int i = 0; i < args.length; i++) {

            if (args[i].equals("-h") || args[i].equals("--help")) {
                usage(System.out);
                System.exit(0);
            } else if (args[i].equals("-w") || args[i].equals("--waitnodes")) {
                waitNodes = Integer.parseInt(args[++i]);
            } else {
                taskCount = Integer.parseInt(args[i]);
                goForMaestro = true;
            }
        }
        System.out.println("Running on platform " + Utils.getPlatformVersion()
                + " args.length=" + args.length + " goForMaestro="
                + goForMaestro + "; taskCount=" + taskCount);
        try {
            new MasterWorkerProgram().run(taskCount, goForMaestro, waitNodes);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
