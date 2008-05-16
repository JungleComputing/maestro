package image;

public interface ImageCompressor {
    public CompressedImage addImage(UncompressedImage image) throws Exception;
    public CompressedImage flush() throws Exception;
    public String getType();
}
