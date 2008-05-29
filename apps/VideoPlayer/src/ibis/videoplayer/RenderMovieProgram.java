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
    
    private final class FetchImageAction implements Job {
        private static final long serialVersionUID = -7976035811697720295L;

        /**
         *
         * @param in The input of this job.
         * @param node The node we're running on.
         * @param context The program context of this job.
         * @return The fetched image.
         */
        
        public Object run( Object in, Node node, Context context ) {
            File f = (File) in;
            try {
            return UncompressedImage.load( f, 0 );
            }
            catch( IOException e ) {
                System.err.println( "Cannot read image file: " + e.getLocalizedMessage() );
                return null;
            }
        }   
    }

    private final class ColorCorrectAction implements Job
    {
        private static final long serialVersionUID = 5452987225377415308L;
        final double rr, rg, rb;
        final double gr, gg, gb;
        final double br, bg, bb;
        
        ColorCorrectAction(final double rr, final double rg, final double rb, final double gr, final double gg, final double gb, final double br, final double bg, final double bb) {
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

            return img.colorCorrect(rr, rg, rb, gr, gg, gb, br, bg, bb );
        }
    }

    private final class ScaleFrameAction implements Job
    {
        private static final long serialVersionUID = 5452987225377415309L;
        final int scale;
        
        ScaleFrameAction( int scale )
        {
            this.scale = scale;
        }

        /**
         * Run a scale down Maestro job.
         * @param in The input of this job.
         * @param node The node this job runs on.
         * @param context The program context of this job.
         * @return The result of the job.
         */
        @Override
        public Object run( Object in, Node node, Context context ) {
            UncompressedImage img = (UncompressedImage) in;
            
            return img.scaleDown( scale );
        }
    }

    private final class CompressFrameAction implements Job
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
    private void run( File sourceDirectory, File destinationDirectory ) throws Exception
    {
        Node node = new Node( new ConverterContext( sourceDirectory ), sourceDirectory != null );
        TaskWaiter waiter = new TaskWaiter();
        Task convertTask =  node.createTask(
                "converter",
                new FetchImageAction(),
                new ColorCorrectAction( 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0 ),
                new ScaleFrameAction( 3 ),
                new CompressFrameAction()
        );

        System.out.println( "Node created" );
        if( sourceDirectory != null ) {
            File files[] = sourceDirectory.listFiles();
            System.out.println( "I am maestro; converting " + files.length + " images" );
            for( File f: files ) {
                waiter.submit( convertTask, f );
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
        File inputDir = null;
        File outputDir = null;

        if( args.length == 1 ){
            System.err.println( "Missing parameter: I need an input AND an output directory, or nothing'" );
            System.exit( 1 );
        }
        if( args.length>1 ) {
            inputDir = new File( args[0] );
            outputDir = new File( args[1] );
            if( !inputDir.exists() ) {
                System.err.println( "Input directory '" + inputDir + "' does not exist" );
                System.exit( 1 );
            }
            if( !inputDir.isDirectory() ) {
                System.err.println( "Input directory '" + inputDir + "' is not a directory" );
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
        System.out.println( "Running on platform " + Service.getPlatformVersion() + " input=" + inputDir + " output=" + outputDir );
        try {
            new RenderMovieProgram().run( inputDir, outputDir );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
