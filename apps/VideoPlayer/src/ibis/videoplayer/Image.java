package ibis.videoplayer;

abstract class Image {

    protected final int width;
    protected final int height;
    protected final int frameno;

    public Image( int width, int height, int frameno )
    {
        this.width = width;
        this.height = height;
        this.frameno = frameno;
    }

    abstract Image scale();
}