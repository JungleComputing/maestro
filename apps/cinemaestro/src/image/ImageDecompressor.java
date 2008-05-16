package image;

public interface ImageDecompressor {

    public UncompressedImage decompress(CompressedImage image) throws Exception;
    public String getType();
}
