package ibis.videoplayer;

import ibis.maestro.AtomicTask;
import ibis.maestro.Job;
import ibis.maestro.JobList;
import ibis.maestro.JobWaiter;
import ibis.maestro.Node;

import java.io.File;
import java.io.IOException;

/**
 * Run some conversions on a directory full of images.
 * 
 * @author Kees van Reeuwijk
 *
 */
class ConvertFramesProgram {    
    private final class FetchImageTask implements AtomicTask {
        private static final long serialVersionUID = -7976035811697720295L;

        /**
         *
         * @param in The input of this job.
         * @param node The node we're running on.
         * @return The fetched image.
         */
        
        public Object run( Object in, Node node ) {
            File f = (File) in;
            try {
            return UncompressedImage.load( f, 0 );
            }
            catch( IOException e ) {
                System.err.println( "Cannot read image file: " + e.getLocalizedMessage() );
                return null;
            }
        }

        /**
         * @return True, because this job can run anywhere.
         */
        @Override
        public boolean isSupported()
        {
            return true;
        }
    }

    private final class ColorCorrectTask implements AtomicTask
    {
        private static final long serialVersionUID = 5452987225377415308L;
        final double rr, rg, rb;
        final double gr, gg, gb;
        final double br, bg, bb;
        
        ColorCorrectTask(final double rr, final double rg, final double rb, final double gr, final double gg, final double gb, final double br, final double bg, final double bb) {
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
         * @return THe converted image.
         */
        @Override
        public Object run( Object in, Node node ) {
            UncompressedImage img = (UncompressedImage) in;

            return img.colourCorrect(rr, rg, rb, gr, gg, gb, br, bg, bb );
        }

        /**
         * @return True, because this job can run anywhere.
         */
        @Override
        public boolean isSupported()
        {
            return true;
        }
    }

    private final class CompressFrameTask implements AtomicTask
    {
        private static final long serialVersionUID = 5452987225377415310L;

        /**
         * Run a Jpeg conversion Maestro task.
         * @param in The input of this task.
         * @param node The node this job runs on.
         * @return The result of the task.
         */
        @Override
        public Object run( Object in, Node node ) {
            UncompressedImage img = (UncompressedImage) in;

            try {
                return JpegCompressedImage.convert( img );
            } catch (IOException e) {
                System.err.println( "Cannot convert image to JPEG: " + e.getLocalizedMessage() );
                e.printStackTrace();
                return null;
            }
        }

        /**
         * @return True, because this job can run anywhere.
         */
        @Override
        public boolean isSupported()
        {
            return true;
        }
    }

    @SuppressWarnings("synthetic-access")
    private void run( File framesDirectory ) throws Exception
    {
        JobList tasks = new JobList();
        JobWaiter waiter = new JobWaiter();
        Job convertTask =  tasks.createJob(
                "converter",
                new FetchImageTask(),
                new ColorCorrectTask( 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0 ),
                new ScaleFrameTask( 2 ),
                new CompressFrameTask()
        );

        Node node = new Node( tasks, framesDirectory != null );
        System.out.println( "Node created" );
        if( framesDirectory != null ) {
            File files[] = framesDirectory.listFiles();
            System.out.println( "I am maestro; converting " + files.length + " images" );
            for( File f: files ) {
                waiter.submit( node, convertTask, f );
            }
	    node.setStopped();
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
            new ConvertFramesProgram().run( inputDir );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
