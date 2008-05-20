package ibis.videoplayer;

import java.io.IOException;

interface ImageCompressor {
    /**
     * Given an uncompressed image, returns a compressed version.
     * @param image The image to compress.
     * @return The compressed image.
     * @throws IOException Thrown iff there is a problem during compression.
     */
    CompressedImage compress( UncompressedImage image ) throws IOException;
}
