package image;

public class NullCompressedImage extends CompressedImage {

    /** 
     * Generated
     */
    private static final long serialVersionUID = -6341581445807596496L;

    public NullCompressedImage(long number, Object metaData) {
        super(number, metaData, "NULL");
    }

    @Override
    public Object getData() {
        return null;
    }

    @Override
    public long getSize() {
        return 0;
    }

}
