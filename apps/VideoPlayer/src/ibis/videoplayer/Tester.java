package ibis.videoplayer;

import java.io.File;
import java.io.IOException;

public class Tester {

    /**
     * Simple wrapper to test methods without all the Maestro infrastructure.
     * @param args The command-line arguments
     */
    public static void main(String[] args) {
        try {
            RGB48Image red = RGB48Image.fillImage( 0, 100, 100, (short) 0xFFFF, (short) 0, (short) 0 );
            red.write( new File( "test-red.png" ) );
//            UncompressedImage img = UncompressedImage.load( new File( "test.png" ), 0 );
//            System.out.println( "read image " + img );
//            img.write( new File( "test-out.png" ) );
//            UncompressedImage img24 = RGB24Image.convert( img );
//            System.out.println( "downsampled to image " + img24 );
//            CompressedImage cimg = JPEGCompressor.compress( img24 );
//            cimg.write( new File( "test.jpg" ) );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
