package ibis.videoplayer;

import java.io.File;
import java.io.IOException;

/**
 * Test videoplayer routines.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class TestProgram
{

    /**
     * Simple wrapper to test methods without all the Maestro infrastructure.
     * @param args The command-line arguments
     */
    public static void main( String[] args ) {
        try {
            UncompressedImage img = UncompressedImage.load( new File( "images/japan-tuin-8bit.ppm" ), 0 );
            System.out.println( "Loaded " + img );
            //img = img.scaleUp( 3 );
            img = img.sharpen();

//          String inc = RenderFrameJob.readFile( new File( "frames/context.inc" ) );
//          String scene = RenderFrameJob.readFile( new File( "frames/S01-frame000000.pov" ) );
//          System.out.println( "Loaded scene" );
//          UncompressedImage img24 = RenderFrameJob.renderImage( 1500, 500, 0, 1000, 0, 500, 0, inc + scene );
//          System.out.println( "Rendered scene" );

            // UncompressedImage img = UncompressedImage.load(  new File( "images/japan-tuin-16bit.ppm" ), 0 );
            System.out.println( "scaled to " + img );
//          System.out.println( "read image " + img );
            img.write( new File( "test-out.ppm" ) );
//          img24 = RGB24Image.convert( img24 );
//          System.out.println( "downsampled to image " + img24 );
//          CompressedImage cimg = JpegCompressedImage.convert( img24 );
//          System.out.println( "compressed to " + cimg );
//          cimg.write( new File( "test.jpg" ) );
            System.out.println( "Finished" );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
