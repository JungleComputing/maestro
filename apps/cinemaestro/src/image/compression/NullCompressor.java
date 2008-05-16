package image.compression;

import image.CompressedImage;
import image.ImageCompressor;
import image.NullCompressedImage;
import image.UncompressedImage;

public class NullCompressor implements ImageCompressor {
    
    public NullCompressor() { 
    }
    
    public CompressedImage addImage(UncompressedImage image) throws Exception {
        // We simply discard all of the image expept the number and metadata
        return new NullCompressedImage(image.number, image.metaData);
    }

    public CompressedImage flush() throws Exception {
        // Nothing to do here...
        return null;
    }

    public String getType() {
        return "NULL";
    }
}
