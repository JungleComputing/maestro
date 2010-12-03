package ibis.videoplayer;

import ibis.maestro.AtomicJob;
import ibis.maestro.Job;
import ibis.maestro.JobCompletionListener;
import ibis.maestro.JobExecutionTimeEstimator;
import ibis.maestro.JobList;
import ibis.maestro.LabelTracker;
import ibis.maestro.LabelTracker.Label;
import ibis.maestro.Node;
import ibis.maestro.ParallelJob;
import ibis.maestro.ParallelJobContext;
import ibis.maestro.ParallelJobHandler;
import ibis.maestro.ParallelJobInstance;
import ibis.maestro.SeriesJob;
import ibis.maestro.Utils;
import ibis.steel.ConstantEstimate;
import ibis.steel.Estimate;
import ibis.steel.LogGaussianEstimate;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Run some conversions on a directory full of images.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class BenchmarkProgram {
    static final int DVD_WIDTH = 720;
    static final int DVD_HEIGHT = 576;
    static Job scalerJob;

    static final File outputDir = new File("output");

    private static class Listener implements JobCompletionListener {
        private final LabelTracker labelTracker = new LabelTracker();
        private boolean sentFinal = false;

        /**
         * Handle the completion of the job with id 'id': the result is
         * 'result'.
         * 
         * @param id
         *            The job that was completed.
         * @param result
         *            The result of the job.
         */
        @Override
        public void jobCompleted(final Node node, final Object id,
                final Serializable result) {
            if (!(id instanceof LabelTracker.Label)) {
                System.err
                        .println("Internal error: Object id is not a tracker label: "
                                + id);
                System.exit(1);
            }
            labelTracker.returnLabel((LabelTracker.Label) id);
            boolean finished;
            synchronized (this) {
                finished = sentFinal && labelTracker.allAreReturned();
            }
            if (finished) {
                System.out
                        .println("I got all job results back; stopping test program");
                node.setStopped();
            }
            final long returned = labelTracker.getReturnedLabels();
            final long issued = labelTracker.getIssuedLabels();
            if (returned % 500 == 0) {
                System.out.println("Now " + returned + " of " + issued
                        + " frames returned");
            }
            if (issued - returned < 5) {
                final Label[] l = labelTracker.listOutstandingLabels();
                System.out.println("Waiting for " + Arrays.deepToString(l));
            }
        }

        Serializable getLabel() {
            return labelTracker.nextLabel();
        }

        void setFinished(final Node node) {
            boolean finished;
            synchronized (this) {
                sentFinal = true;
                finished = labelTracker.allAreReturned();
            }
            if (finished) {
                System.out
                        .println("I got all job results back; stopping test program");
                node.setStopped();
            }
        }
    }

    /** Empty class to send around when there is nothing to say. */
    static final class Empty implements Serializable {
        private static final long serialVersionUID = 2;
    }

    // Do all the image processing steps in one go. Used as baseline.
    private static final class ProcessFrameJob implements AtomicJob,
            JobExecutionTimeEstimator {
        private static final long serialVersionUID = -7976035811697720295L;
        final boolean slowScale;
        final boolean slowSharpen;
        final File saveDir;

        ProcessFrameJob(final boolean slowScale, final boolean slowSharpen,
                final File saveDir) {
            this.slowScale = slowScale;
            this.slowSharpen = slowSharpen;
            this.saveDir = saveDir;
        }

        /**
         * 
         * @param in
         *            The input of this job.
         * @return <code>null</code> since we entirely process the image within
         *         this job.
         */
        @Override
        public Serializable run(final Serializable in) {
            final int frame = (Integer) in;

            UncompressedImage img = RGB24Image.buildGradientImage(frame,
                    DVD_WIDTH, DVD_HEIGHT, (byte) frame, (byte) (frame / 10),
                    (byte) (frame / 100));
            if (slowScale) {
                img.scaleUp(2);
                img.scaleUp(2);
                img.scaleUp(2);
            }
            img = img.scaleUp(2);
            if (slowSharpen) {
                img = img.sharpen();
                img = img.sharpen();
                img = img.sharpen();
            }
            img = img.sharpen();
            try {
                final CompressedImage cimg = JpegCompressedImage.convert(img);
                if (saveDir != null) {
                    if (!saveDir.isDirectory()) {
                        saveDir.mkdir();
                    }
                    final File f = new File(saveDir, String.format(
                            "frame%05d.jpg", img.frameno));
                    cimg.write(f);
                }
                return new Empty();
            } catch (final IOException e) {
                System.err.println("Cannot compress image: "
                        + e.getLocalizedMessage());
            }
            return null;
        }

        /**
         * @return True, because this job can run anywhere.
         */
        @Override
        public boolean isSupported() {
            return true;
        }

        /**
         * Estimates the time to execute this job. (Overrides method in
         * superclass.) We simply time the actual execution of frame generation,
         * so this is as accurate as it gets.
         * 
         * @return The estimated time in ns to execute this job.
         */
        @Override
        public Estimate estimateJobExecutionTime() {
            final double startTime = Utils.getPreciseTime();
            run(0);
            final double d = Math.max(1e-9, Utils.getPreciseTime() - startTime);
            return new LogGaussianEstimate(Math.log(d), Math.log(10), 1);
        }
    }

    private static final class GenerateFrameJob implements AtomicJob,
            JobExecutionTimeEstimator {
        private static final long serialVersionUID = -7976035811697720295L;

        /**
         * @param in
         *            The input of this job: the frame number.
         * @return The generated image.
         */
        @Override
        public Serializable run(final Serializable in) {
            final int frame = (Integer) in;
            return generateFrame(frame);
        }

        private static Serializable generateFrame(final int frame) {
            return RGB24Image.buildGradientImage(frame, DVD_WIDTH, DVD_HEIGHT,
                    (byte) frame, (byte) (frame / 10), (byte) (frame / 100));
        }

        /**
         * @return True, because this job can run anywhere.
         */
        @Override
        public boolean isSupported() {
            return true;
        }

        /**
         * Estimates the time to execute this job. (Overrides method in
         * superclass.) We simply time the actual execution of frame generation,
         * so this is as accurate as it gets.
         * 
         * @return The estimated time in ns to execute this job.
         */
        @Override
        public Estimate estimateJobExecutionTime() {
            final double startTime = Utils.getPreciseTime();
            generateFrame(0);
            final double d = Math.max(1e-9, Utils.getPreciseTime() - startTime);
            return new LogGaussianEstimate(Math.log(d), Math.log(10), 1);
        }
    }

    private static final class ScaleUpFrameJob implements AtomicJob,
            JobExecutionTimeEstimator {
        private static final long serialVersionUID = 5452987225377415308L;
        private final int factor;
        private final boolean slow;
        private final boolean allowed;

        ScaleUpFrameJob(final int factor, final boolean slow,
                final boolean allowed) {
            this.factor = factor;

            this.slow = slow;
            this.allowed = allowed;
            if (allowed) {
                if (slow) {
                    System.out.println("Using slow upscaling");
                }
            } else {
                System.out.println("Upscaling not allowed");
            }
        }

        /**
         * Scale up one frame in a Maestro flow.
         * 
         * @param in
         *            The input of the conversion.
         * @return The scaled frame.
         */
        @Override
        public Serializable run(final Serializable in) {
            final UncompressedImage img = (UncompressedImage) in;

            if (slow) {
                img.scaleUp(factor);
                img.scaleUp(factor);
                img.scaleUp(factor);
            }
            return img.scaleUp(factor);
        }

        /**
         * @return True iff the allowed flag is set for this class.
         */
        @Override
        public boolean isSupported() {
            return allowed;
        }

        /**
         * Estimates the time to execute this job. (Overrides method in
         * superclass.) We simply time the actual execution of frame generation,
         * so this is as accurate as it gets.
         * 
         * @return The estimated time in ns to execute this job.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public Estimate estimateJobExecutionTime() {
            if (!allowed) {
                return null;
            }
            final Serializable frame = GenerateFrameJob.generateFrame(0);
            final double startTime = Utils.getPreciseTime();
            run(frame);
            final double d = Math.max(1e-9, Utils.getPreciseTime() - startTime);
            return new LogGaussianEstimate(Math.log(d), Math.log(10), 1);
        }
    }

    static class ParallelScaler implements ParallelJob {
        static final int FRAGMENT_COUNT = 9;

        @Override
        public boolean isSupported() {
            return scalerJob.isSupported();
        }

        static class ParallelScalerInstance extends ParallelJobInstance {
            UncompressedImage fragments[] = new UncompressedImage[FRAGMENT_COUNT];

            ParallelScalerInstance(final ParallelJobContext context) {
                super(context);
            }

            @Override
            public Serializable getResult() {
                return UncompressedImage.concatenateImagesVertically(fragments);
            }

            @Override
            public void merge(final Serializable id, final Serializable result) {
                final int ix = (Integer) id;
                fragments[ix] = (UncompressedImage) result;
            }

            /**
             * Returns true iff the result is ready. In this case, when we have
             * all the fragments.
             * 
             * @return True iff we have all fragments.
             */
            @Override
            public boolean resultIsReady() {
                for (final UncompressedImage fragment : fragments) {
                    if (fragment == null) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void split(final Serializable input,
                    final ParallelJobHandler handler) {
                final UncompressedImage img = (UncompressedImage) input;

                final UncompressedImage l[] = img
                        .splitVertically(FRAGMENT_COUNT);
                for (int i = 0; i < l.length; i++) {
                    handler.submit(l[i], this, i, scalerJob);
                }
            }

        }

        @Override
        public ParallelJobInstance createInstance(
                final ParallelJobContext context) {
            return new ParallelScalerInstance(context);
        }

    }

    private static final class SharpenFrameJob implements AtomicJob,
            JobExecutionTimeEstimator {
        private static final long serialVersionUID = 54529872253774153L;
        private final boolean slow;
        private final boolean allowed;

        SharpenFrameJob(final boolean slow, final boolean allowed) {
            this.slow = slow;
            this.allowed = allowed;
            if (allowed) {
                if (slow) {
                    System.out.println("Using slow sharpen");
                }
            } else {
                System.out.println("Sharpen not allowed");
            }
        }

        /**
         * Sharpen one frame in a Maestro flow.
         * 
         * @param in
         *            The input of the conversion.
         * @return The scaled frame.
         */
        @Override
        public Serializable run(final Serializable in) {
            UncompressedImage img = (UncompressedImage) in;

            if (!allowed) {
                System.err.println("Sharpen job invoked, although not allowed");
            }
            if (slow) {
                img = img.sharpen();
                img = img.sharpen();
                img = img.sharpen();
            }
            return img.sharpen();
        }

        /**
         * @return True iff allowed is set.
         */
        @Override
        public boolean isSupported() {
            return allowed;
        }

        /**
         * Estimates the time to execute this job. (Overrides method in
         * superclass.) We simply time the actual execution of frame generation,
         * so this is as accurate as it gets.
         * 
         * @return The estimated time in s to execute this job.
         */
        @SuppressWarnings("synthetic-access")
        @Override
        public Estimate estimateJobExecutionTime() {
            if (!allowed) {
                return null;
            }
            final Serializable frame = GenerateFrameJob.generateFrame(0);
            final double startTime = Utils.getPreciseTime();
            run(frame);
            final double d = Math.max(1e-9,
                    4 * (Utils.getPreciseTime() - startTime));
            return new LogGaussianEstimate(Math.log(d), Math.log(10), 1);
        }
    }

    private static final class CompressFrameJob implements AtomicJob {
        private static final long serialVersionUID = 5452987225377415310L;

        /**
         * Run a Jpeg conversion Maestro job.
         * 
         * @param in
         *            The input of this job.
         * @return The result of the job.
         */
        @Override
        public Serializable run(final Serializable in) {
            final UncompressedImage img = (UncompressedImage) in;

            try {
                return JpegCompressedImage.convert(img);
            } catch (final IOException e) {
                System.err.println("Cannot convert image to JPEG: "
                        + e.getLocalizedMessage());
                e.printStackTrace();
                return null;
            }
        }

        /**
         * @return True, because this job can run anywhere.
         */
        @Override
        public boolean isSupported() {
            return true;
        }
    }

    private static final class SaveFrameJob implements AtomicJob,
            JobExecutionTimeEstimator {
        private static final long serialVersionUID = 54529872253774153L;
        private final File saveDir;

        SaveFrameJob(final File saveDir) {
            this.saveDir = saveDir;
        }

        /**
         * Optionally save one frame in a Maestro flow. This job is placed at
         * the end of a benchmark flow, and normally just ignores the received
         * image. Optionally it can store the image for debugging purposes.
         * 
         * @param in
         *            The input of the conversion.
         * @return The scaled frame.
         */
        @Override
        public Serializable run(final Serializable in) {
            final Image img = (Image) in;

            if (saveDir != null) {
                if (!saveDir.isDirectory()) {
                    saveDir.mkdir();
                }
                final File f = new File(saveDir, String.format("frame%05d.jpg",
                        img.frameno));
                try {
                    img.write(f);
                } catch (final IOException e) {
                    e.printStackTrace();
                }
            }
            return new Empty();
        }

        /**
         * @return True, because this job can run anywhere.
         */
        @Override
        public boolean isSupported() {
            return true;
        }

        /**
         * Estimates the execution time of this job. (Overrides method in
         * superclass.)
         * 
         * @return The estimated time on ns to execute this job.
         */
        @Override
        public Estimate estimateJobExecutionTime() {
            // Saving a file may take some time, but otherwise the estimate
            // should be zero.
            if (saveDir != null) {
                // TODO: better estimate for save step.
                return new LogGaussianEstimate(Math.log(10e-3), Math.log(10), 1);
            }
            return ConstantEstimate.ZERO;
        }
    }

    private boolean goForMaestro = false;
    private int frames = 0;
    private boolean saveFrames = false;
    private boolean slowSharpen = false;
    private boolean slowScale = false;
    private boolean allowSharpen = true;
    private boolean parallelScaling = false;
    private boolean allowScale = true;
    private boolean oneJob = false;

    private static void printUsage() {

        System.out.println("Usage: [<flags>] <frame-count>");
    }

    private boolean parseArgs(final String args[]) {
        String frameCount = null;
        boolean oddNoSharpen = false;
        boolean evenNoSharpen = false;
        boolean oddNoScale = false;
        boolean evenNoScale = false;
        boolean oddSlowScale = false;
        boolean evenSlowScale = false;
        boolean oddSlowSharpen = false;
        boolean evenSlowSharpen = false;

        for (final String arg : args) {
            if (arg.equalsIgnoreCase("-h") || arg.equalsIgnoreCase("-help")) {
                printUsage();
                return false;
            }
            if (arg.equalsIgnoreCase("-save")) {
                saveFrames = true;
            } else if (arg.equalsIgnoreCase("-parallelscaling")) {
                parallelScaling = true;
            } else if (arg.equalsIgnoreCase("-onejob")) {
                oneJob = true;
            } else if (arg.equalsIgnoreCase("-nosharpen")) {
                allowSharpen = true;
            } else if (arg.equalsIgnoreCase("-oddnosharpen")) {
                oddNoSharpen = true;
            } else if (arg.equalsIgnoreCase("-evennosharpen")) {
                evenNoSharpen = true;
            } else if (arg.equalsIgnoreCase("-noscale")) {
                allowScale = false;
            } else if (arg.equalsIgnoreCase("-slowsharpen")) {
                slowSharpen = true;
            } else if (arg.equalsIgnoreCase("-oddslowsharpen")) {
                oddSlowSharpen = true;
            } else if (arg.equalsIgnoreCase("-evenslowsharpen")) {
                evenSlowSharpen = true;
            } else if (arg.equalsIgnoreCase("-oddnoscale")) {
                oddNoScale = true;
            } else if (arg.equalsIgnoreCase("-evennoscale")) {
                evenNoScale = true;
            } else if (arg.equalsIgnoreCase("-slowscale")) {
                slowScale = true;
            } else if (arg.equalsIgnoreCase("-oddslowscale")) {
                oddSlowScale = true;
            } else if (arg.equalsIgnoreCase("-evenslowscale")) {
                evenSlowScale = true;
            } else {
                if (frameCount != null) {
                    System.err.println("Duplicate frame count. Was: ["
                            + frameCount + "] new: [" + arg + "]");
                    return false;
                }
                frameCount = arg;
            }
        }
        if (oddSlowScale || oddSlowSharpen || evenSlowScale || evenSlowSharpen
                || oddNoScale || evenNoScale || oddNoSharpen || evenNoSharpen) {
            final String env = System.getenv(Settings.RANK);
            if (env == null) {
                System.err
                        .println("Environment variable "
                                + Settings.RANK
                                + " not set, so I don't know if this node is odd or even");
                return false;
            }
            final int rank = Integer.parseInt(env);
            if (rank % 2 == 0) {
                if (evenNoScale) {
                    allowScale = false;
                }
                if (evenNoSharpen) {
                    allowSharpen = false;
                }
                if (evenSlowSharpen) {
                    slowSharpen = true;
                }
                if (evenSlowScale) {
                    slowScale = true;
                }
            } else {
                if (oddNoScale) {
                    allowScale = false;
                }
                if (oddNoSharpen) {
                    allowSharpen = false;
                }
                if (oddSlowSharpen) {
                    slowSharpen = true;
                }
                if (oddSlowScale) {
                    slowScale = true;
                }
            }
        }
        if (frameCount != null) {
            frames = Integer.parseInt(frameCount);
            goForMaestro = true;
        }
        return true;
    }

    private static void removeDirectory(final File f) {
        if (f == null) {
            return;
        }
        if (f.isFile()) {
            f.delete();
        } else if (f.isDirectory()) {
            for (final File e : f.listFiles()) {
                removeDirectory(e);
            }
            f.delete();
        }
    }

    @SuppressWarnings("synthetic-access")
    private void run(final String args[]) throws Exception {
        if (!parseArgs(args)) {
            System.err.println("Parsing command line failed. Goodbye!");
            System.exit(1);
        }
        System.out.println("frames=" + frames + " goForMaestro=" + goForMaestro
                + " saveFrames=" + saveFrames + " oneJob=" + oneJob
                + " slowSharpen=" + slowSharpen + " slowScale=" + slowScale
                + " parallelScaling=" + parallelScaling);
        final JobList jobs = new JobList();
        SeriesJob convertJob;
        final Listener listener = new Listener();
        final File dir = saveFrames ? outputDir : null;
        if (oneJob) {
            if (!allowScale || !allowSharpen) {
                System.err
                        .println("Disabling steps is meaningless in a one-job benchmark");
                System.exit(1);
            }
            System.out.println("One-job benchmark");
            final ProcessFrameJob processFrameJob = new ProcessFrameJob(
                    slowScale, slowSharpen, dir);
            convertJob = new SeriesJob(processFrameJob);
        } else {
            Job scaleUpFrameJob;
            if (parallelScaling) {
                scalerJob = new ScaleUpFrameJob(2, slowScale, allowScale);
                jobs.registerJob(scalerJob);
                scaleUpFrameJob = new ParallelScaler();
            } else {
                scaleUpFrameJob = new ScaleUpFrameJob(2, slowScale, allowScale);
            }
            convertJob = new SeriesJob(new GenerateFrameJob(), scaleUpFrameJob,
                    new SharpenFrameJob(slowSharpen, allowSharpen),
                    new CompressFrameJob(), new SaveFrameJob(dir));
        }
        jobs.registerJob(convertJob);
        final Node node = Node.createNode(jobs, goForMaestro);

        removeDirectory(dir);

        System.out.println("Node created");
        final long startTime = System.nanoTime();
        if (node.isMaestro()) {
            for (int frame = 0; frame < frames; frame++) {
                final Serializable label = listener.getLabel();
                node.submit(frame, label, listener, convertJob);
            }
            listener.setFinished(node);
            System.out.println("Jobs submitted");
        }
        node.waitToTerminate();
        final long stopTime = System.nanoTime();
        System.out.println("DURATION " + (stopTime - startTime));
    }

    /**
     * The command-line interface of this program.
     * 
     * @param args
     *            The list of command-line parameters.
     */
    public static void main(final String args[]) {
        try {
            new BenchmarkProgram().run(args);
        } catch (final Exception e) {
            System.err.println("main() caught an exception");
            e.printStackTrace(System.err);
        }
    }
}
