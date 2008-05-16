package image;

public class TIFCompressedImage extends CompressedImage {

    /** 
     * Generated
     */
    private static final long serialVersionUID = 2943319117224074964L;

    private final byte [] data;
    
    public TIFCompressedImage(long number, Object metaData, byte [] data) {
        super(number, metaData, "TIF");
        this.data = data;
    }
    
    @Override
    public Object getData() {
        return data;
    }
    
    @Override
    public long getSize() { 
        return data.length;
    }
}
