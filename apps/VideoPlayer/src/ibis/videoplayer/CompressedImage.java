package ibis.videoplayer;

public class CompressedImage extends Image {

    public CompressedImage( int width, int height, int frameno )
    {
        super(width, height, frameno);
    }
    
    Image scale()
    {
        System.err.println( "Scaling of compressed images not supported" );
        return null;
    }
}
