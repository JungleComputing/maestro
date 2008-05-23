package ibis.videoplayer;

import java.io.File;
import java.io.IOException;

/**
 * Test videoplayer routines.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Tester {

    /**
     * Simple wrapper to test methods without all the Maestro infrastructure.
     * @param args The command-line arguments
     */
    public static void main(String[] args) {
        try {
            UncompressedImage img48 = UncompressedImage.load( new File( "foto2.ppm" ), 0 );
            System.out.println( "read image " + img48 );
            UncompressedImage img24 = RGB24Image.convert( img48 );
            System.out.println( "converted to " + img24 );
//            System.out.println( "read image " + img );
//            img.write( new File( "test-out.png" ) );
//            UncompressedImage img24 = RGB24Image.convert( img );
//            System.out.println( "downsampled to image " + img24 );
            CompressedImage cimg = JpegCompressedImage.convert( img24 );
            System.out.println( "compressed to " + cimg );
            cimg.write( new File( "test.jpg" ) );
            System.out.println( "Finished" );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
