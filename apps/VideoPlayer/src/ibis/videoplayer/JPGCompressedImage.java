package ibis.videoplayer;

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
}
