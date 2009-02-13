package ibis.videoplayer;

import ibis.maestro.AtomicJob;
import ibis.maestro.JobCompletionListener;
import ibis.maestro.JobExecutionTimeEstimator;
import ibis.maestro.JobList;
import ibis.maestro.JobSequence;
import ibis.maestro.LabelTracker;
import ibis.maestro.Node;
import ibis.maestro.Utils;
import ibis.maestro.LabelTracker.Label;

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

    static final File outputDir = new File("output");

    private static class Listener implements JobCompletionListener {
        private final LabelTracker labelTracker = new LabelTracker();
        private boolean sentFinal = false;

        /**
         * Handle the completion of the job with id 'id':
         * the result is 'result'.
         * 
         * @param id
         *            The job that was completed.
         * @param result
         *            The result of the job.
         */
        @Override
        public void jobCompleted(Node node, Object id, Object result) {
            if (!(id instanceof LabelTracker.Label)) {
                System.err
                .println("Internal error: Object id is not a tracker label: "
                        + id);
                System.exit(1);
            }
            labelTracker.returnLabel((LabelTracker.Label) id);
            boolean finished;
            synchronized( this ){
                finished = sentFinal && labelTracker.allAreReturned();
            }
            if (finished) {
                System.out
                .println("I got all job results back; stopping test program");
                node.setStopped();
            }
            final long returned = labelTracker.getReturnedLabels();
            final long issued = labelTracker.getIssuedLabels();
            if( (returned%500) == 0 ){
                System.out.println( "Now " + returned + " of " + issued + " frames returned" );
            }
            if( (issued-returned)<20 ){
                final Label[] l = labelTracker.listOutstandingLabels();
                System.out.println( "Waiting for " + Arrays.deepToString(l) );
            }
        }

        Serializable getLabel() {
            return labelTracker.nextLabel();
        }

        void setFinished(Node node) {
            boolean finished;
            synchronized( this ){
                sentFinal = true;
                finished = labelTracker.allAreReturned();
            }
            if( finished ){
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
    private static final class ProcessFrameTask implements AtomicJob, JobExecutionTimeEstimator {
        private static final long serialVersionUID = -7976035811697720295L;
        final boolean slowScale;
        final boolean slowSharpen;
        final File saveDir;

        ProcessFrameTask(final boolean slowScale, final boolean slowSharpen,
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
        public Object run(Object in) {
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
        public double estimateTaskExecutionTime() {
            final double startTime = Utils.getPreciseTime();
            run(0);
            return Utils.getPreciseTime() - startTime;
        }
    }

    private static final class GenerateFrameTask implements AtomicJob,
    JobExecutionTimeEstimator {
        private static final long serialVersionUID = -7976035811697720295L;

        /**
         * @param in
         *            The input of this job: the frame number.
         * @return The generated image.
         */
        public Object run(Object in) {
            final int frame = (Integer) in;
            return generateFrame(frame);
        }

        private static Object generateFrame(int frame) {
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
        public double estimateTaskExecutionTime() {
            final double startTime = Utils.getPreciseTime();
            generateFrame(0);
            return Utils.getPreciseTime() - startTime;
        }
    }

    private static final class ScaleUpFrameTask implements AtomicJob,
    JobExecutionTimeEstimator {
        private static final long serialVersionUID = 5452987225377415308L;
        private final int factor;
        private final boolean slow;
        private final boolean allowed;

        ScaleUpFrameTask(int factor, boolean slow, boolean allowed) {
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
        public Object run(Object in) {
            final UncompressedImage img = (UncompressedImage) in;

            if (slow) {
                img.scaleUp(factor);
                img.scaleUp(factor);
                img.scaleUp(factor);
            }
            final Object res = img.scaleUp(factor);
            return res;
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
        public double estimateTaskExecutionTime() {
            if( !allowed ){
                return Double.POSITIVE_INFINITY;
            }
            final Object frame = GenerateFrameTask.generateFrame(0);
            final double startTime = Utils.getPreciseTime();
            run( frame );
            return Utils.getPreciseTime() - startTime;
        }
    }

    private static final class SharpenFrameTask implements AtomicJob, JobExecutionTimeEstimator {
        private static final long serialVersionUID = 54529872253774153L;
        private final boolean slow;
        private final boolean allowed;

        SharpenFrameTask(boolean slow, boolean allowed) {
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
        public Object run(Object in) {
            UncompressedImage img = (UncompressedImage) in;

            if( !allowed ){
                System.err.println( "Sharpen job invoked, although not allowed" );
            }
            if( slow ) {
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
        public double estimateTaskExecutionTime() {
            if( !allowed ){
                return Double.POSITIVE_INFINITY;
            }
            final Object frame = GenerateFrameTask.generateFrame(0);
            final double startTime = Utils.getPreciseTime();
            run( frame );
            return 4*(Utils.getPreciseTime() - startTime);
        }
    }

    private static final class CompressFrameTask implements AtomicJob {
        private static final long serialVersionUID = 5452987225377415310L;

        /**
         * Run a Jpeg conversion Maestro job.
         * 
         * @param in
         *            The input of this job.
         * @return The result of the job.
         */
        @Override
        public Object run(Object in) {
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

    private static final class SaveFrameTask implements AtomicJob,
    JobExecutionTimeEstimator {
        private static final long serialVersionUID = 54529872253774153L;
        private final File saveDir;

        SaveFrameTask(File saveDir) {
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
        public Object run(Object in) {
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
        public double estimateTaskExecutionTime() {
            // Saving a file may take some time, but otherwise the estimate
            // should be zero.
            if (saveDir != null) {
                // TODO: better estimate for save step.
                return 10e-3; // 10 ms
            }
            return 0;
        }
    }

    private boolean goForMaestro = false;
    private int frames = 0;
    private boolean saveFrames = false;
    private boolean slowSharpen = false;
    private boolean slowScale = false;
    private boolean allowSharpen = true;
    private boolean allowScale = true;
    private boolean oneJob = false;

    private static void printUsage() {

        System.out.println("Usage: [<flags>] <frame-count>");
    }

    private boolean parseArgs(String args[]) {
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
        if( oddSlowScale || oddSlowSharpen || evenSlowScale || evenSlowSharpen || oddNoScale || evenNoScale || oddNoSharpen || evenNoSharpen ) {
            final String env = System.getenv( Settings.RANK );
            if( env == null ) {
                System.err.println( "Environment variable " + Settings.RANK + " not set, so I don't know if this node is odd or even" );
                return false;
            }
            final int rank = Integer.parseInt( env );
            if( (rank%2) == 0 ) {
                if( evenNoScale ) {
                    allowScale = false;
                }
                if( evenNoSharpen ) {
                    allowSharpen = false;
                }
                if( evenSlowSharpen ) {
                    slowSharpen = true;
                }
                if( evenSlowScale  ) {
                    slowScale = true;
                }
            }
            else {
                if( oddNoScale ) {
                    allowScale = false;
                }
                if( oddNoSharpen ) {
                    allowSharpen = false;
                }
                if( oddSlowSharpen ) {
                    slowSharpen = true;
                }
                if( oddSlowScale ) {
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

    private static void removeDirectory(File f) {
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
    private void run(String args[]) throws Exception {
        if (!parseArgs(args)) {
            System.err.println("Parsing command line failed. Goodbye!");
            System.exit(1);
        }
        System.out.println("frames=" + frames + " goForMaestro=" + goForMaestro
                + " saveFrames=" + saveFrames + " oneJob=" + oneJob
                + " slowSharpen=" + slowSharpen + " slowScale=" + slowScale);
        final JobList jobs = new JobList();
        JobSequence convertJob;
        final Listener listener = new Listener();
        final File dir = saveFrames ? outputDir : null;
        if (oneJob) {
            if (!allowScale || !allowSharpen) {
                System.err
                .println("Disabling steps is meaningless in a one-job benchmark");
                System.exit(1);
            }
            System.out.println("One-job benchmark");
            convertJob = jobs.createJobSequence( new ProcessFrameTask(
                    slowScale, slowSharpen, dir));
        } else {
            convertJob = jobs.createJobSequence( new GenerateFrameTask(),
                    new ScaleUpFrameTask(2, slowScale, allowScale),
                    new SharpenFrameTask(slowSharpen, allowSharpen),
                    new CompressFrameTask(), new SaveFrameTask(dir));
        }
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
    public static void main(String args[]) {
        try {
            new BenchmarkProgram().run(args);
        } catch (final Exception e) {
            System.err.println("main() caught an exception");
            e.printStackTrace(System.err);
        }
    }
}
