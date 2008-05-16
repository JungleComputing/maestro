package image;

public abstract class RGBImage extends UncompressedImage {

    public RGBImage(long number, int width, int height, Object metaData) {
        super(number, width, height, metaData);
    }
    
    public abstract YUVImage toYUV();  
    
}
