package ibis.maestro;

import java.io.Serializable;

/**
 * Small test program.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class OneTestProg {
    private static final int ITERATIONS = 8000; // The number of times we should

    // do the addition.
    private static final int ARRAY_SIZE = 100000;

    private static class Listener implements JobCompletionListener {
        int tasksCompleted = 0;

        private final int taskCount;

        Listener(int taskCount) {
            this.taskCount = taskCount;
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
            tasksCompleted++;
            // System.out.println( "I now have " + tasksCompleted + "/" +
            // taskCount + " tasks" );
            if (tasksCompleted >= taskCount) {
                System.out
                        .println("I got all task results back; stopping test program");
                node.setStopped();
            }
        }
    }

    private static final class Empty implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private static class CreateArrayTask implements AtomicTask {
        private static final long serialVersionUID = 2347248108353357517L;

        /**
         * Returns the name of this task.
         * 
         * @return The name.
         */
        @Override
        public String getName() {
            return "Create array";
        }

        /**
         * Runs this task.
         * 
         * @param obj
         *            The input parameter of this task.
         * @return The result value of this task.
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
    private void run(int taskCount, boolean goForMaestro) throws Exception {
        Listener listener = new Listener(taskCount);
        JobList jobs = new JobList();

        Job job = jobs.createJob("testprog", new CreateArrayTask());
        Node node = Node.createNode(jobs, goForMaestro);
        System.out.println("Node created");
        double startTime = Utils.getPreciseTime();
        if (node.isMaestro()) {
            System.out.println("I am maestro; submitting " + taskCount
                    + " tasks");
            for (int i = 0; i < taskCount; i++) {
                Integer length = 12 * i;
                node.submit(length, i, true, listener, job);
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
        int taskCount = 0;

        if (args.length == 0) {
            System.err
                    .println("Missing parameter: I need a task count, or 'worker'");
            System.exit(1);
        }
        String arg = args[0];
        if (arg.equalsIgnoreCase("worker")) {
            goForMaestro = false;
        } else {
            taskCount = Integer.parseInt(arg);
        }
        System.out.println("Running on platform " + Utils.getPlatformVersion()
                + " args.length=" + args.length + " goForMaestro="
                + goForMaestro + "; taskCount=" + taskCount);
        try {
            new OneTestProg().run(taskCount, goForMaestro);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
