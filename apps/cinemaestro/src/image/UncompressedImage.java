package image;

public abstract class UncompressedImage extends Image {

    public final int width;
    public final int height;
    
    public UncompressedImage(long number, int width, int height, Object metaData) {
        super(number, metaData);
        this.width = width;
        this.height = height;
    }
    
    public abstract UncompressedImage scale(int w, int h);
    public abstract void colorAdjust(double r, double g, double b);
    public abstract UncompressedImage scale(int width, int height, int bits);
   
}
