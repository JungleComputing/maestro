package ibis.videoplayer;

import ibis.maestro.AtomicJob;
import ibis.maestro.JobCompletionListener;
import ibis.maestro.JobList;
import ibis.maestro.LabelTracker;
import ibis.maestro.Node;
import ibis.maestro.SeriesJob;
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
class ConvertFramesProgram {
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
        public void jobCompleted(Node node, Object id, Serializable result) {
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

    private final class FetchImageJob implements AtomicJob {
        private static final long serialVersionUID = -7976035811697720295L;

        /**
         * 
         * @param in
         *            The input of this job.
         * @return The fetched image.
         */

        public Serializable run(Serializable in) {
            final File f = (File) in;
            try {
                return Image.load(f, 0);
            } catch (final IOException e) {
                System.err.println("Cannot read image file: "
                        + e.getLocalizedMessage());
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

    private final class ColorCorrectJob implements AtomicJob {
        private static final long serialVersionUID = 5452987225377415308L;
        final double rr, rg, rb;
        final double gr, gg, gb;
        final double br, bg, bb;

        ColorCorrectJob(final double rr, final double rg, final double rb,
                final double gr, final double gg, final double gb,
                final double br, final double bg, final double bb) {
            super();
            this.rr = rr;
            this.rg = rg;
            this.rb = rb;
            this.gr = gr;
            this.gg = gg;
            this.gb = gb;
            this.br = br;
            this.bg = bg;
            this.bb = bb;
        }

        /**
         * Color-correct one image in a Maestro flow.
         * 
         * @param in
         *            The input of the conversion.
         * @return THe converted image.
         */
        @Override
        public Serializable run(Serializable in) {
            final UncompressedImage img = (UncompressedImage) in;

            return img.colourCorrect(rr, rg, rb, gr, gg, gb, br, bg, bb);
        }

        /**
         * @return True, because this job can run anywhere.
         */
        @Override
        public boolean isSupported() {
            return true;
        }
    }

    private final class CompressFrameJob implements AtomicJob {
        private static final long serialVersionUID = 5452987225377415310L;

        /**
         * Run a Jpeg conversion Maestro job.
         * 
         * @param in
         *            The input of this job.
         * @return The result of the job.
         */
        @Override
        public Serializable run(Serializable in) {
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

    @SuppressWarnings("synthetic-access")
    private void run(File framesDirectory) throws Exception {
        final JobList jobs = new JobList();
        final Listener listener = new Listener();
        final SeriesJob convertJob = new SeriesJob(new FetchImageJob(),
                new ColorCorrectJob(1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0,
                        1.0), new ScaleFrameJob(2), new CompressFrameJob());

        jobs.registerJob(convertJob);
        final Node node = Node.createNode(jobs, framesDirectory != null);
        System.out.println("Node created");
        if (framesDirectory != null) {
            final File files[] = framesDirectory.listFiles();
            System.out.println("I am maestro; converting " + files.length
                    + " images");
            for (final File f : files) {
                final Serializable label = listener.getLabel();
                node.submit(f,label,listener,convertJob);
            }
            listener.setFinished( node );
        }
        node.waitToTerminate();
    }

    /**
     * The command-line interface of this program.
     * 
     * @param args
     *            The list of command-line parameters.
     */
    public static void main(String args[]) {
        File inputDir = null;
        File outputDir = null;

        if (args.length == 1) {
            System.err
            .println("Missing parameter: I need an input AND an output directory, or nothing'");
            System.exit(1);
        }
        if (args.length > 1) {
            inputDir = new File(args[0]);
            outputDir = new File(args[1]);
            if (!inputDir.exists()) {
                System.err.println("Input directory '" + inputDir
                        + "' does not exist");
                System.exit(1);
            }
            if (!inputDir.isDirectory()) {
                System.err.println("Input directory '" + inputDir
                        + "' is not a directory");
                System.exit(1);
            }
            if (!outputDir.exists()) {
                if (!outputDir.mkdir()) {
                    System.err.println("Cannot create output directory '"
                            + outputDir + "'");
                    System.exit(1);
                }
            }
            if (!outputDir.isDirectory()) {
                System.err.println("Output directory '" + outputDir
                        + "' is not a directory");
                System.exit(1);
            }
        }
        System.out.println("Running on platform "
                + Service.getPlatformVersion() + " input=" + inputDir
                + " output=" + outputDir);
        try {
            new ConvertFramesProgram().run(inputDir);
        } catch (final Exception e) {
            e.printStackTrace(System.err);
        }
    }
}
