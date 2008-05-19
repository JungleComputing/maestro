package image;

public final class VoidImage extends Image {
    private static final long serialVersionUID = 3836133276350388325L;
    public final int width;
    public final int height;
    
    public VoidImage(long number, int width, int height, Object metaData) {
        super(number, metaData);
        this.width = width;
        this.height = height;
    }

    @Override
    public Object getData() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getSize() {
        // TODO Auto-generated method stub
        return 0;
    }
   
}
