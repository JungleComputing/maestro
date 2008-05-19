package ibis.videoplayer;

import java.awt.image.BufferedImage;

public abstract class UncompressedImage extends Image {

    public UncompressedImage(int width, int height, int frameno) {
        super(width, height, frameno);
    }

    abstract BufferedImage toBufferedImage();

}