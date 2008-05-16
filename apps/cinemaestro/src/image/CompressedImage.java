package image;

public abstract class CompressedImage extends Image {

    public final String compression;
    
    public CompressedImage(long number, Object metaData, String compression) {
        super(number, metaData);
        this.compression = compression;
    }

    public String getCompression() { 
        return compression;
    }
}