package ibis.videoplayer;

import ibis.maestro.CompletionListener;
import ibis.maestro.Context;
import ibis.maestro.Job;
import ibis.maestro.Node;
import ibis.maestro.Task;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Construct a movie from a directory full of povray scripts.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RenderMovieProgram implements CompletionListener
{
    static final int WIDTH = 348;
    static final int HEIGHT = 216;
    static final int REPEATS = 1;
    static final double RENDER_TIME = 1.0;  // Pessimistic estimated time in seconds to render a frame.
    static final double SHOW_INTERVAL = 0.5; // Time in seconds from the first frame submission until planned show. 
    static final int FRAMES_PER_SECOND = 25;

    private final LinkedList<FrameSchedule> queue = new LinkedList<FrameSchedule>();
    private int outstandingJobs = 0;
    private final File outputDir;

    RenderMovieProgram( File outputDir )
    {
        this.outputDir = outputDir;
    }

    private void submitNeededFrames( long now, Task task )
    {
        long renderDeadline = now+(long) (RENDER_TIME*1e9);
        synchronized( queue ) {
            while( !queue.isEmpty() ) {
                FrameSchedule fr = queue.getFirst();

                if( fr.showMoment<renderDeadline ) {
                    // This one is required soon, put it in the queue.
                    System.out.println( "Submitting frame " + fr.frameno );
                    RenderFrameJob.RenderInfo info = new RenderFrameJob.RenderInfo( WIDTH, HEIGHT, 0, WIDTH, 0, HEIGHT, fr.frameno, fr.scene );
                    task.submit( info, new Integer( fr.frameno ), this );
                    outstandingJobs++;
                }
            }
        }
    }

    private class ConverterContext implements Context {
        final File sourceDirectory;

        ConverterContext(final File sourceDirectory) {
            this.sourceDirectory = sourceDirectory;
        }

        File getSourceDirectory()
        {
            return sourceDirectory;
        }
    }

    private final class ColorCorrectJob implements Job
    {
        private static final long serialVersionUID = 5452987225377415308L;
        final double rr, rg, rb;
        final double gr, gg, gb;
        final double br, bg, bb;

        ColorCorrectJob(final double rr, final double rg, final double rb, final double gr, final double gg, final double gb, final double br, final double bg, final double bb) {
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

        /** Color-convert one image in a Maestro flow.
         * 
         * @param in The input of the conversion.
         * @param node The node this process runs on.
         * @param context The program context.
         * @return THe converted image.
         */
        @Override
        public Object run( Object in, Node node, Context context ) {
            UncompressedImage img = (UncompressedImage) in;

            System.out.println( "Colour-correcting " + img );
            return img.colourCorrect(rr, rg, rb, gr, gg, gb, br, bg, bb );
        }
    }


    private final class DownsampleJob implements Job
    {
        private static final long serialVersionUID = 5452987225377415308L;

        /** Downsample one image in a Maestro flow.
         * 
         * @param in The input of the conversion.
         * @param node The node this process runs on.
         * @param context The program context.
         * @return THe converted image.
         */
        @Override
        public Object run( Object in, Node node, Context context ) {
            UncompressedImage img = (UncompressedImage) in;

            System.out.println( "Downsampling " + img );
            return RGB24Image.convert( img );
        }
    }

    private final class CompressFrameJob implements Job
    {
        private static final long serialVersionUID = 5452987225377415310L;

        /**
         * Run a Jpeg conversion Maestro job.
         * @param in The input of this job.
         * @param node The node this job runs on.
         * @param context The program context of this job.
         * @return The result of the job.
         */
        @Override
        public Object run( Object in, Node node, Context context ) {
            UncompressedImage img = (UncompressedImage) in;

            try {
                System.out.println( "Compressing " + img );
                return JpegCompressedImage.convert( img );
            } catch (IOException e) {
                System.err.println( "Cannot convert image to JPEG: " + e.getLocalizedMessage() );
                e.printStackTrace();
                return null;
            }
        }
    }

    private static class FrameSchedule {
        final String scene;
        final int frameno;
        final long showMoment;

        /**
         * Given a scene and show moment, constructs a new FrameSchedule.
         * @param scene The scene to render
         * @param frameno The frame number of the frame.
         * @param showMoment The moment to show the rendered scene.
         */
        public FrameSchedule( String scene, int frameno, long showMoment )
        {
            this.scene = scene;
            this.frameno = frameno;
            this.showMoment = showMoment;
        }
    }

    @SuppressWarnings("synthetic-access")
    private void run( File sourceDirectory, File iniFile ) throws Exception
    {
        Node node = new Node( new ConverterContext( sourceDirectory ), sourceDirectory != null );
        Task convertTask =  node.createTask(
            "converter",
            new RenderFrameJob(),
            new ColorCorrectJob( 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0 ),
            //new ScaleFrameJob( 2 ),
            new DownsampleJob(),
            new CompressFrameJob()
        );

        int frameno = 0;
        System.out.println( "Node created" );
        if( sourceDirectory != null ) {
            String init = RenderFrameJob.readFile( iniFile );
            if( init == null ) {
                System.err.println( "Cannot read file " + iniFile );
                return;
            }
            File files[] = sourceDirectory.listFiles();
            System.out.println( "I am maestro; converting " + files.length + " images" );
            for( int iter=0; iter<REPEATS; iter++ ) {
                for( File f: files ) {
                    if( !f.getName().equals( ".svn" ) ) {
                        String scene = RenderFrameJob.readFile( f );
                        if( scene == null ) {
                            System.err.println( "Cannot read scene file " + f );
                        }
                        else {
                            int n = frameno++;
                            RenderFrameJob.RenderInfo info = new RenderFrameJob.RenderInfo( WIDTH, HEIGHT, 0, WIDTH, 0, HEIGHT, n, init + scene );
                            convertTask.submit( info, new Integer( n ), this );
                            System.out.println( "Submitted frame " + n );
                            outstandingJobs++;
                        }
                    }
                }
            }
        }
        node.waitToTerminate();
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] )
    {
        if( args.length != 0 && args.length != 3 ){
            System.err.println( "Usage: <input-directory> <init-file> <output-directory>" );
            System.err.println( "Or give no parameters at all for a helper node" );
            System.exit( 1 );
        }
        File inputDir = null;
        File initFile = null;
        File outputDir = null;
        if( args.length != 0 ) {
            inputDir = new File( args[0] );
            initFile = new File( args[1] );
            outputDir = new File( args[2] );
            if( !inputDir.exists() ) {
                System.err.println( "Input directory '" + inputDir + "' does not exist" );
                System.exit( 1 );
            }
            if( !inputDir.isDirectory() ) {
                System.err.println( "Input directory '" + inputDir + "' is not a directory" );
                System.exit( 1 );
            }
            if( !initFile.exists() ) {
                System.err.println( "Init file '" + initFile + "' does not exist" );
                System.exit( 1 );
            }
            if( !initFile.isFile() ) {
                System.err.println( "Init file '" + initFile + "' is not a file" );
                System.exit( 1 );
            }
            if( !outputDir.exists() ) {
                if( !outputDir.mkdir() ) {
                    System.err.println( "Cannot create output directory '" + outputDir + "'" );
                    System.exit( 1 );
                }
            }
            if( !outputDir.isDirectory() ) {
                System.err.println( "Output directory '" + outputDir + "' is not a directory" );
                System.exit( 1 );
            }
        }
        System.out.println( "Running on platform " + Service.getPlatformVersion() + " input=" + inputDir + " init=" + initFile + " output=" + outputDir );
        try {
            new RenderMovieProgram( outputDir ).run( inputDir, initFile );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }

    /**
     * Handles the completion of a task. (Overrides method in superclass.)
     * @param node The node we're running on.
     * @param id The id of the completed task.
     * @param result The result of the task.
     */
    @Override
    public void taskCompleted( Node node, Object id, Object result )
    {
        int frameno = (Integer) id;
        Image img = (Image) result;

        File f = new File( outputDir, String.format( "f%06d.jpg", frameno ) );
        try {
            img.write( f );
        }
        catch( IOException e ) {
            System.out.println( "Cannot write result file " + f + ": " + e.getLocalizedMessage() );
        }
        outstandingJobs--;
        if( outstandingJobs == 0 ) {
            node.setStopped();
        }
    }
}
