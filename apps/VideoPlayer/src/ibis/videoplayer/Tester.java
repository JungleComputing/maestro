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
            UncompressedImage img = UncompressedImage.load( new File( "test.png" ), 0 );
            System.out.println( "read image " + img );
            CompressedImage cimg = JPEGCompressor.compress( img );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
