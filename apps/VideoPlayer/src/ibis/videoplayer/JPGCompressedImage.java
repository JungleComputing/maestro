package ibis.videoplayer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

class JPGCompressedImage extends CompressedImage {

    /** 
     * Generated
     */
    private static final long serialVersionUID = 2943319117224074964L;

    private final byte [] data;
    
    JPGCompressedImage( int width, int height, int frameno, byte [] data) {
        super( width, height, frameno );
        this.data = data;
    }

    /**
     * 
     * Writes the image to the given file.
     * @param f The file to write to.
     */
    @Override
    void write( File f ) throws IOException
    {
        FileOutputStream out = new FileOutputStream( f );
        out.write( data );
        out.close();

    }
}
