package ibis.videoplayer;

abstract class CompressedImage extends Image {
    private static final long serialVersionUID = -825288928748291555L;

    CompressedImage( int width, int height, int frameno )
    {
        super(width, height, frameno);
    }
    
    @Override
    Image scale()
    {
        System.err.println( "Scaling of compressed images not supported" );
        return null;
    }
}
