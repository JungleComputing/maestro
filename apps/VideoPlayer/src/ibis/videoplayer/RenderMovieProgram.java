package ibis.videoplayer;

import ibis.maestro.Context;
import ibis.maestro.Job;
import ibis.maestro.Node;
import ibis.maestro.Task;
import ibis.maestro.TaskWaiter;

import java.io.File;
import java.io.IOException;

/**
 * Construct a movie from a directory full of povray scripts.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RenderMovieProgram {
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
                return JpegCompressedImage.convert( img );
            } catch (IOException e) {
                System.err.println( "Cannot convert image to JPEG: " + e.getLocalizedMessage() );
                e.printStackTrace();
                return null;
            }
        }
    }

    @SuppressWarnings("synthetic-access")
    private void run( File sourceDirectory, File iniFile, File destinationDirectory ) throws Exception
    {
        String init = RenderFrameJob.readFile( iniFile );
        if( init == null ) {
            System.err.println( "Cannot read file " + iniFile );
            return;
        }
        Node node = new Node( new ConverterContext( sourceDirectory ), sourceDirectory != null );
        TaskWaiter waiter = new TaskWaiter();
        Task convertTask =  node.createTask(
            "converter",
            new RenderFrameJob(),
            new ColorCorrectJob( 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0 ),
            new ScaleFrameJob( 2 ),
            new DownsampleJob(),
            new CompressFrameJob()
        );

        int frameno = 0;
        final int width = 200;
        final int height = 100;
        System.out.println( "Node created" );
        if( sourceDirectory != null ) {
            File files[] = sourceDirectory.listFiles();
            System.out.println( "I am maestro; converting " + files.length + " images" );
            for( File f: files ) {
                if( !f.getName().equals( ".svn" ) ) {
                    String scene = RenderFrameJob.readFile( f );
                    if( scene == null ) {
                        System.err.println( "Cannot read scene file " + f );
                    }
                    else {
                        RenderFrameJob.RenderInfo info = new RenderFrameJob.RenderInfo( width, height, 0, width, 0, height, frameno++, init + scene );
                        waiter.submit( convertTask, info );
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
        if( args.length != 3 ){
            System.err.println( "Usage: <input-directory> <init-file> <output-directory>" );
            System.exit( 1 );
        }
        File inputDir = new File( args[0] );
        File initFile = new File( args[1] );
        File outputDir = new File( args[2] );
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
        System.out.println( "Running on platform " + Service.getPlatformVersion() + " input=" + inputDir + " init=" + initFile + " output=" + outputDir );
        try {
            new RenderMovieProgram().run( inputDir, initFile, outputDir );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
